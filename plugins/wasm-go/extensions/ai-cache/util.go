package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-cache/config"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

func handleNonStreamChunk(ctx wrapper.HttpContext, c config.PluginConfig, chunk []byte, log log.Log) error {
	tempContentI := ctx.GetContext(CACHE_CONTENT_CONTEXT_KEY)
	if tempContentI == nil {
		ctx.SetContext(CACHE_CONTENT_CONTEXT_KEY, chunk)
		return nil
	}
	tempContent := tempContentI.([]byte)
	tempContent = append(tempContent, chunk...)
	ctx.SetContext(CACHE_CONTENT_CONTEXT_KEY, tempContent)
	return nil
}

func unifySSEChunk(data []byte) []byte {
	data = bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
	data = bytes.ReplaceAll(data, []byte("\r"), []byte("\n"))
	return data
}

func handleStreamChunk(ctx wrapper.HttpContext, c config.PluginConfig, chunk []byte, log log.Log) error {
	var partialMessage []byte
	partialMessageI := ctx.GetContext(PARTIAL_MESSAGE_CONTEXT_KEY)
	log.Debugf("[handleStreamChunk] cache content: %v", ctx.GetContext(CACHE_CONTENT_CONTEXT_KEY))
	if partialMessageI != nil {
		partialMessage = append(partialMessageI.([]byte), chunk...)
	} else {
		partialMessage = chunk
	}
	messages := strings.Split(string(partialMessage), "\n\n")
	for i, msg := range messages {
		if i < len(messages)-1 {
			_, err := processSSEMessage(ctx, c, msg, log)
			if err != nil {
				return fmt.Errorf("[handleStreamChunk] processSSEMessage failed, error: %v", err)
			}
		}
	}
	if !strings.HasSuffix(string(partialMessage), "\n\n") {
		ctx.SetContext(PARTIAL_MESSAGE_CONTEXT_KEY, []byte(messages[len(messages)-1]))
	} else {
		ctx.SetContext(PARTIAL_MESSAGE_CONTEXT_KEY, nil)
	}
	return nil
}

func processNonStreamLastChunk(ctx wrapper.HttpContext, c config.PluginConfig, chunk []byte, log log.Log) (string, error) {
	var body []byte
	tempContentI := ctx.GetContext(CACHE_CONTENT_CONTEXT_KEY)
	if tempContentI != nil {
		body = append(tempContentI.([]byte), chunk...)
	} else {
		body = chunk
	}
	bodyJson := gjson.ParseBytes(body)
	value := bodyJson.Get(c.CacheValueFrom).String()
	if value == "" && isResponsesAPI(ctx) {
		value = extractResponsesOutputText(bodyJson)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("[processNonStreamLastChunk] parse value from response body failed, body:%s", body)
	}
	return value, nil
}

func isResponsesAPIPath(path string) bool {
	path = strings.TrimRight(path, "/")
	if queryIndex := strings.Index(path, "?"); queryIndex >= 0 {
		path = path[:queryIndex]
	}
	return strings.HasSuffix(path, "/responses")
}

func isResponsesAPI(ctx wrapper.HttpContext) bool {
	apiType, _ := ctx.GetContext(API_TYPE_CONTEXT_KEY).(string)
	return apiType == API_TYPE_RESPONSES
}

func extractResponsesCacheKey(bodyJson gjson.Result, strategy string) string {
	input := bodyJson.Get("input")
	if !input.Exists() {
		return ""
	}
	if input.Type == gjson.String {
		return input.String()
	}
	items := input.Array()
	switch strategy {
	case config.CACHE_KEY_STRATEGY_LAST_QUESTION:
		for i := len(items) - 1; i >= 0; i-- {
			if text := extractResponsesInputText(items[i], true); text != "" {
				return text
			}
		}
	case config.CACHE_KEY_STRATEGY_ALL_QUESTIONS:
		var userMessages []string
		for _, item := range items {
			if text := extractResponsesInputText(item, true); text != "" {
				userMessages = append(userMessages, text)
			}
		}
		return strings.Join(userMessages, "\n")
	}
	return ""
}

func extractResponsesInputText(item gjson.Result, requireUserRole bool) string {
	if requireUserRole && item.Get("role").String() != "user" {
		return ""
	}
	return extractResponsesContentText(item.Get("content"), "input_text")
}

func extractResponsesOutputText(bodyJson gjson.Result) string {
	if outputText := bodyJson.Get("output_text").String(); outputText != "" {
		return outputText
	}
	var contents []string
	for _, output := range bodyJson.Get("output").Array() {
		if output.Get("type").String() != "message" {
			continue
		}
		if text := extractResponsesContentText(output.Get("content"), "output_text"); text != "" {
			contents = append(contents, text)
		}
	}
	return strings.Join(contents, "")
}

func extractResponsesContentText(content gjson.Result, textType string) string {
	if !content.Exists() {
		return ""
	}
	if content.Type == gjson.String {
		return content.String()
	}
	var texts []string
	for _, part := range content.Array() {
		if part.Get("type").String() == textType {
			texts = append(texts, part.Get("text").String())
		}
	}
	return strings.Join(texts, "")
}

func processStreamLastChunk(ctx wrapper.HttpContext, c config.PluginConfig, chunk []byte, log log.Log) (string, error) {
	if len(chunk) > 0 {
		var lastMessage []byte
		partialMessageI := ctx.GetContext(PARTIAL_MESSAGE_CONTEXT_KEY)
		if partialMessageI != nil {
			lastMessage = append(partialMessageI.([]byte), chunk...)
		} else {
			lastMessage = chunk
		}
		if !strings.HasSuffix(string(lastMessage), "\n\n") {
			return "", fmt.Errorf("[processStreamLastChunk] invalid lastMessage:%s", lastMessage)
		}
		lastMessage = lastMessage[:len(lastMessage)-2]
		value, err := processSSEMessage(ctx, c, string(lastMessage), log)
		if err != nil {
			return "", fmt.Errorf("[processStreamLastChunk] processSSEMessage failed, error: %v", err)
		}
		return value, nil
	}
	tempContentI := ctx.GetContext(CACHE_CONTENT_CONTEXT_KEY)
	if tempContentI == nil {
		return "", nil
	}
	return tempContentI.(string), nil
}

func processSSEMessage(ctx wrapper.HttpContext, c config.PluginConfig, sseMessage string, log log.Log) (string, error) {
	content := ""
	for _, chunk := range strings.Split(sseMessage, "\n\n") {
		log.Debugf("single sse message: %s", chunk)
		subMessages := strings.Split(chunk, "\n")
		var message string
		for _, msg := range subMessages {
			if strings.HasPrefix(msg, "data:") {
				message = msg
				break
			}
		}
		if len(message) < 6 {
			return content, fmt.Errorf("[processSSEMessage] invalid message: %s", message)
		}

		// skip the prefix "data:"
		bodyJson := message[5:]

		if strings.TrimSpace(bodyJson) == "[DONE]" {
			return content, nil
		}

		if isResponsesAPI(ctx) {
			responseContent := extractResponsesStreamText(bodyJson)
			if responseContent != "" {
				content += responseContent
			}
			if isResponsesToolCallStreamEvent(bodyJson) {
				ctx.SetContext(TOOL_CALLS_CONTEXT_KEY, bodyJson)
			}
			continue
		}

		// Extract values from JSON fields
		responseBody := gjson.Get(bodyJson, c.CacheStreamValueFrom)
		toolCalls := gjson.Get(bodyJson, c.CacheToolCallsFrom)

		if toolCalls.Exists() {
			// TODO: Temporarily store the tool_calls value in the context for processing
			ctx.SetContext(TOOL_CALLS_CONTEXT_KEY, toolCalls.String())
		}

		// Check if the ResponseBody field exists
		if !responseBody.Exists() {
			if ctx.GetContext(CACHE_CONTENT_CONTEXT_KEY) != nil {
				log.Debugf("[processSSEMessage] unable to extract content from message; cache content is not nil: %s", message)
				return content, nil
			}
			return content, fmt.Errorf("[processSSEMessage] unable to extract content from message; cache content is nil: %s", message)
		} else {
			content += responseBody.String()
		}
	}
	tempContentI := ctx.GetContext(CACHE_CONTENT_CONTEXT_KEY)
	// If there is no content in the cache, initialize and set the content
	if tempContentI == nil {
		ctx.SetContext(CACHE_CONTENT_CONTEXT_KEY, content)
	} else {
		ctx.SetContext(CACHE_CONTENT_CONTEXT_KEY, tempContentI.(string)+content)
	}
	return content, nil
}

func extractResponsesStreamText(bodyJson string) string {
	if gjson.Get(bodyJson, "type").String() != "response.output_text.delta" {
		return ""
	}
	return gjson.Get(bodyJson, "delta").String()
}

func isResponsesToolCallStreamEvent(bodyJson string) bool {
	eventType := gjson.Get(bodyJson, "type").String()
	return strings.Contains(eventType, "function_call") || strings.Contains(eventType, "tool_call")
}
