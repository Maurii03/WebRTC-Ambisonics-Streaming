package room

import "sync"

// Manager owns the set of live rooms, keyed by room id, creating them on first
// join and destroying them when the last participant leaves. It supports multiple
// independent rooms (each with its own mix clock), which Phase 4 exercises.
type Manager struct {
	mu        sync.Mutex
	rooms     map[string]*Room
	channels  int
	frameSize int
}

// NewManager builds a room manager. channels/frameSize are applied to every room
// it creates (the frame size is server-authoritative for the whole server).
func NewManager(channels, frameSize int) *Manager {
	return &Manager{
		rooms:     make(map[string]*Room),
		channels:  channels,
		frameSize: frameSize,
	}
}

// Join adds p to the room named roomID, creating (and starting) the room if it
// does not exist yet.
func (m *Manager) Join(roomID string, p Participant) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rm := m.rooms[roomID]
	if rm == nil {
		rm = NewRoom(roomID, m.channels, m.frameSize)
		m.rooms[roomID] = rm
	}
	rm.Add(p) // lock order is always manager → room; Add does not block
}

// Leave removes the participant id from roomID and tears the room down (stopping
// its mix clock) once it is empty. Safe to call even if the participant never
// joined.
func (m *Manager) Leave(roomID, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rm := m.rooms[roomID]
	if rm == nil {
		return
	}
	rm.Remove(id)
	if rm.Size() == 0 {
		rm.Close()
		delete(m.rooms, roomID)
	}
}

// Rooms reports the number of live rooms (telemetry/tests).
func (m *Manager) Rooms() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}

// Snapshot returns a telemetry snapshot of every live room (for /metrics).
func (m *Manager) Snapshot() []RoomMetrics {
	m.mu.Lock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, rm := range m.rooms {
		rooms = append(rooms, rm)
	}
	m.mu.Unlock() // release before calling Room.Metrics (which takes each room's lock)

	out := make([]RoomMetrics, 0, len(rooms))
	for _, rm := range rooms {
		out = append(out, rm.Metrics())
	}
	return out
}
