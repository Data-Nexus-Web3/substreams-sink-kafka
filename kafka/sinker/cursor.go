package sinker

import (
	"fmt"
	"os"

	"github.com/streamingfast/substreams/sink"
)

func (s *KafkaSinker) loadCursor() (*sink.Cursor, error) {
	if s.cursorFile == "" {
		return sink.NewBlankCursor(), nil
	}

	// Check if file exists
	if _, err := os.Stat(s.cursorFile); os.IsNotExist(err) {
		return sink.NewBlankCursor(), nil
	}

	// Read cursor from file
	data, err := os.ReadFile(s.cursorFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read cursor from file %s: %w", s.cursorFile, err)
	}

	if len(data) == 0 {
		return sink.NewBlankCursor(), nil
	}

	cursor, err := sink.NewCursor(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse cursor from file %s: %w", s.cursorFile, err)
	}

	return cursor, nil
}

func (s *KafkaSinker) saveCursor(cursor *sink.Cursor) error {
	if s.cursorFile == "" {
		return nil // No cursor file specified
	}

	cursorStr := cursor.String()
	if err := os.WriteFile(s.cursorFile, []byte(cursorStr), 0644); err != nil {
		return fmt.Errorf("failed to write cursor to file %s: %w", s.cursorFile, err)
	}

	return nil
}
