package userclient

import (
	"encoding/json"
	"strings"
)

// decodeDiscordJSON tolerates component types introduced by Discord before
// discordgo has published support for them. Unknown UI components are not
// archive content, so they can be omitted without dropping the message.
func decodeDiscordJSON(data []byte, dest any) error {
	err := json.Unmarshal(data, dest)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unknown component type") {
		return err
	}
	var value any
	if fallbackErr := json.Unmarshal(data, &value); fallbackErr != nil {
		return err
	}
	stripUnknownComponents(value)
	sanitized, fallbackErr := json.Marshal(value)
	if fallbackErr != nil {
		return err
	}
	return json.Unmarshal(sanitized, dest)
}

func stripUnknownComponents(value any) {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			if key == "components" {
				if components, ok := child.([]any); ok {
					filtered := components[:0]
					for _, component := range components {
						object, ok := component.(map[string]any)
						if !ok || supportedComponentType(object["type"]) {
							stripUnknownComponents(component)
							filtered = append(filtered, component)
						}
					}
					node[key] = filtered
					continue
				}
			}
			stripUnknownComponents(child)
		}
	case []any:
		for _, child := range node {
			stripUnknownComponents(child)
		}
	}
}

func supportedComponentType(value any) bool {
	kind, ok := value.(float64)
	if !ok {
		return true
	}
	return (kind >= 1 && kind <= 14) || kind == 17
}
