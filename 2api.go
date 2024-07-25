package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/lithammer/shortuuid/v4"
	"html"
	"log"
	"net/http"
	"nixiang-gpt/def"
	"nixiang-gpt/s2s"
	"regexp"
	"strings"
	"sync"
	"time"
)

// WebSocket constants
const (
	maxMessageSize = 64 * 1024 // 64 KB
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	waitTimeout    = 30 * time.Second
)
const fnindex = 18

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/v1/chat/completions", handleChatCompletions).Methods("POST")
	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":28888", nil))
}

func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	var req def.OpenAIChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Extract the user message and previous conversations
	var userMessage string
	var previousConversations [][]string
	previousConversations = s2s.ExtractConversations(req.Messages)
	userMessage = previousConversations[len(previousConversations)-1][0]
	previousConversations = previousConversations[:len(previousConversations)-1]
	//每次从req.Messages中提取用户消息和之前的对话，role为user、assistant为一对，system则跳过，如果user后面跟着又是user则说明没有回答，则user自成一对

	client, err := NewClient("wss://academic.jinresearch.site/queue/join")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageChan, err := client.SendMessage(ctx, userMessage, req.Model, previousConversations)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Transfer-Encoding", "chunked")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	var lastResponse string
	var lastEndIdx int
	for {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				return
			}
			//因为后端返回的内容中，针对markdown中的格式显示返回真实的开始例如```，但是后面的内容又会把这里修改为<code>造成错误，所以需要处理
			msg = s2s.ProcessCodeSegments(msg, "<code>")
			msg = s2s.ProcessCodeSegmentsEx(msg, "</code>")
			msg = s2s.DealLine(msg)
			//msg = s2s.ProcessCodeSegments(msg, "</code>")
			//同理，针对代码快结束的位置，openai api是先返回``，然后再返回`\n，但是接口先返回``，后面又变成了</code>，从早晨增量返回没法闭合

			latestResponse := strings.TrimRight(html.UnescapeString(stripHTML(msg)), "\n")
			//latestResponse := s2s.DealRes(msg)
			newPart := latestResponse
			if len(lastResponse) > 0 && len(latestResponse) >= len(lastResponse) {
				newPart = latestResponse[len(lastResponse):]
			} else if len(lastResponse) > 0 {
				newPart = ""
			}
			lastResponse = latestResponse
			if strings.Contains(newPart, "1.") && strings.Contains(newPart, "- ") {
				fmt.Println(newPart)
			}

			tmpEndIdx := strings.Index(msg[lastEndIdx:], ">\n``\n</code>")
			if tmpEndIdx > 0 {
				newPart = strings.ReplaceAll(newPart, "``", "```")
				lastEndIdx = tmpEndIdx
				lastResponse = lastResponse[:len(lastResponse)-2]
			} else {
				tmpEndIdx = strings.Index(msg[lastEndIdx:], ">``<")
				if tmpEndIdx > 0 {
					newPart = strings.ReplaceAll(newPart, "``", "```")
					lastEndIdx = tmpEndIdx
					lastResponse = lastResponse[:len(lastResponse)-2]
				}
			}
			response := def.OpenAIChatResponse{
				Choices: []def.OpenAIChatChoice{{
					Delta: def.OpenAIChatDelta{Content: newPart},
				}},
			}
			data, _ := json.Marshal(response)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

// Existing WebSocket client code
type Client struct {
	conn        *websocket.Conn
	addr        string
	sessionHash string
	sendChan    chan []byte
	receiveChan chan []byte
	mu          sync.Mutex
	isConnected bool
}

func NewClient(addr string) (*Client, error) {
	c := &Client{
		addr:        addr,
		sessionHash: shortuuid.New(),
		sendChan:    make(chan []byte, 256),
		receiveChan: make(chan []byte, 256),
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	go c.readPump()
	go c.writePump()

	return c, nil
}

func (c *Client) connect() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
	}
	conn, _, err := dialer.Dial(c.addr, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	c.conn = conn
	c.isConnected = true
	return nil
}

func (c *Client) SendMessage(ctx context.Context, message string, model string, previousConversations [][]string) (<-chan string, error) {
	responseChan := make(chan string)

	go c.handleMessageExchange(ctx, message, model, previousConversations, responseChan)

	return responseChan, nil
}

func (c *Client) handleMessageExchange(ctx context.Context, message string, model string, previousConversations [][]string, responseChan chan<- string) {
	defer close(responseChan)

	if err := c.performHandshake(); err != nil {
		responseChan <- fmt.Sprintf("Error: %v", err)
		return
	}

	if err := c.sendAiRequest(message, model, previousConversations); err != nil {
		responseChan <- fmt.Sprintf("Error: %v", err)
		return
	}

	c.processResponses(ctx, responseChan)
}

func (c *Client) performHandshake() error {
	if err := c.waitForMessage("send_hash"); err != nil {
		return fmt.Errorf("waiting for send_hash: %w", err)
	}

	if err := c.sendJSON(map[string]interface{}{
		"fn_index":     fnindex,
		"session_hash": c.sessionHash,
	}); err != nil {
		return fmt.Errorf("sending session hash: %w", err)
	}

	if err := c.waitForMessage("estimation"); err != nil {
		return fmt.Errorf("waiting for estimation: %w", err)
	}

	if err := c.waitForMessage("send_data"); err != nil {
		return fmt.Errorf("waiting for send_data: %w", err)
	}

	return nil
}

func (c *Client) sendAiRequest(message string, model string, previousConversations [][]string) error {
	req := map[string]interface{}{
		"data": []interface{}{
			nil, 4096, model, message, "", 1, 1, previousConversations,
			nil, "Serve me as a writing and programming assistant.", "", nil,
		},
		"event_data":   nil,
		"fn_index":     fnindex,
		"session_hash": c.sessionHash,
	}

	return c.sendJSON(req)
}

func (c *Client) processResponses(ctx context.Context, responseChan chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.receiveChan:
			var response def.AiResponse
			if err := json.Unmarshal(msg, &response); err != nil {
				responseChan <- fmt.Sprintf("Error unmarshalling response: %v", err)
				return
			}

			switch response.Msg {
			case "process_starts":
				// Process has started, wait for generating messages
			case "process_generating", "process_completed":
				if !response.Success {
					responseChan <- fmt.Sprintf("Error from server: %v", response.Output)
					return
				}
				if latestResponse := c.extractLatestResponse(response); latestResponse != "" {
					responseChan <- latestResponse
				}
				if response.Msg == "process_completed" {
					return
				}
			default:
				log.Printf("Unexpected message type: %s", response.Msg)
			}
		}
	}
}

func (c *Client) extractLatestResponse(response def.AiResponse) string {
	if len(response.Output.Data) <= 1 {
		return ""
	}

	conversations, ok := response.Output.Data[1].([]interface{})
	if !ok || len(conversations) == 0 {
		return ""
	}

	var latestResponse string
	if len(conversations) == 1 {
		conversation := conversations[0].([]interface{})
		if len(conversation) > 1 {
			latestResponse = conversation[1].(string)
		}
	} else {
		latestConversation := conversations[len(conversations)-1].([]interface{})
		if len(latestConversation) > 1 {
			latestResponse = latestConversation[1].(string)
		}
	}

	return latestResponse
}

func (c *Client) waitForMessage(expectedMsg string) error {
	select {
	case msg := <-c.receiveChan:
		var response def.AiResponse
		if err := json.Unmarshal(msg, &response); err != nil {
			return fmt.Errorf("unmarshalling response: %w", err)
		}
		if response.Msg != expectedMsg {
			return fmt.Errorf("unexpected message: expected %s, got %s", expectedMsg, response.Msg)
		}
		return nil
	case <-time.After(waitTimeout):
		return errors.New("timeout waiting for server message")
	}
}

func (c *Client) sendJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	marshal, err := json.Marshal(v)
	if err != nil {
		return err
	}
	//	log.Printf("Sending: %s", marshal)
	//	return c.conn.WriteMessage(websocket.TextMessage, marshal)
	//	//return c.conn.WriteJSON(v)
	//}
	log.Printf("Sending: %s", marshal)
	return c.conn.WriteMessage(websocket.TextMessage, marshal)
	//return c.conn.WriteJSON(v)
}

func (c *Client) readPump() {
	defer func() {
		c.conn.Close()
		c.isConnected = false
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		c.receiveChan <- message
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.sendChan:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func stripHTML(input string) string {
	//return input
	re := regexp.MustCompile(`<.*?>`)
	return re.ReplaceAllString(input, "")
}
