package watcher

import (
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher manages file watching with a debounce.
type Watcher struct {
	watcher    *fsnotify.Watcher
	debounce   time.Duration
	callbacks  []func(string)
	timers     map[string]*time.Timer
	timersLock sync.Mutex
}

// NewWatcher creates a new debounced watcher.
func NewWatcher(debounce time.Duration) (*Watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		watcher:  fw,
		debounce: debounce,
		timers:   make(map[string]*time.Timer),
	}, nil
}

// Add path to watch.
func (w *Watcher) Add(name string) error {
	return w.watcher.Add(name)
}

// OnChange registers a callback to be called when a file changes.
func (w *Watcher) OnChange(cb func(string)) {
	w.callbacks = append(w.callbacks, cb)
}

// Start begins watching for events.
func (w *Watcher) Start() {
	go func() {
		for {
			select {
			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}
				// We only care about modifications or creations
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
					w.trigger(event.Name)
				}
			case err, ok := <-w.watcher.Errors:
				if !ok {
					return
				}
				log.Println("Watcher error:", err)
			}
		}
	}()
}

func (w *Watcher) trigger(name string) {
	w.timersLock.Lock()
	defer w.timersLock.Unlock()

	// Use absolute path
	absName, err := filepath.Abs(name)
	if err == nil {
		name = absName
	}

	if t, exists := w.timers[name]; exists {
		t.Stop()
	}

	w.timers[name] = time.AfterFunc(w.debounce, func() {
		w.timersLock.Lock()
		delete(w.timers, name)
		w.timersLock.Unlock()

		for _, cb := range w.callbacks {
			cb(name)
		}
	})
}

// Close stops the watcher.
func (w *Watcher) Close() error {
	return w.watcher.Close()
}
