package xconn

import (
	"context"
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/xconnio/wampproto-go"
	"github.com/xconnio/wampproto-go/messages"
)

const (
	MetaProcedureSessionKill           = "wamp.session.kill"
	MetaProcedureSessionCount          = "wamp.session.count"
	MetaProcedureSessionList           = "wamp.session.list"
	MetaProcedureSessionGet            = "wamp.session.get"
	MetaProcedureSessionKillByAuthID   = "wamp.session.kill_by_authid"
	MetaProcedureSessionKillByAuthRole = "wamp.session.kill_by_authrole"
	MetaProcedureSessionKillAll        = "wamp.session.kill_all"

	MetaProcedureRegistrationList         = "wamp.registration.list"
	MetaProcedureRegistrationLookup       = "wamp.registration.lookup"
	MetaProcedureRegistrationMatch        = "wamp.registration.match"
	MetaProcedureRegistrationGet          = "wamp.registration.get"
	MetaProcedureRegistrationListCallees  = "wamp.registration.list_callees"
	MetaProcedureRegistrationCountCallees = "wamp.registration.count_callees"

	MetaTopicSessionJoin  = "wamp.session.on_join"
	MetaTopicSessionLeave = "wamp.session.on_leave"

	MetaTopicRegistrationCreate     = "wamp.registration.on_create"
	MetaTopicRegistrationRegister   = "wamp.registration.on_register"
	MetaTopicRegistrationUnregister = "wamp.registration.on_unregister"
	MetaTopicRegistrationDelete     = "wamp.registration.on_delete"
)

type meta struct {
	router  *Router
	realm   string
	session *Session
}

func newMetAPI(realm string, router *Router) (*meta, error) {
	session, err := ConnectInMemory(router, realm)
	if err != nil {
		return nil, err
	}

	return &meta{
		realm:   realm,
		router:  router,
		session: session,
	}, nil
}

func (m *meta) start() error {
	for uri, handler := range map[string]InvocationHandler{
		MetaProcedureSessionKill:           m.handleSessionKill,
		MetaProcedureSessionCount:          m.handleSessionCount,
		MetaProcedureSessionList:           m.handleSessionList,
		MetaProcedureSessionGet:            m.handleSessionGet,
		MetaProcedureSessionKillByAuthID:   m.handleSessionKillByAuthID,
		MetaProcedureSessionKillByAuthRole: m.handleSessionKillByAuthRole,
		MetaProcedureSessionKillAll:        m.handleSessionKillAll,

		MetaProcedureRegistrationList:         m.handleRegistrationList,
		MetaProcedureRegistrationLookup:       m.handleRegistrationLookup,
		MetaProcedureRegistrationMatch:        m.handleRegistrationMatch,
		MetaProcedureRegistrationGet:          m.handleRegistrationGet,
		MetaProcedureRegistrationListCallees:  m.handleRegistrationListCallees,
		MetaProcedureRegistrationCountCallees: m.handleRegistrationCountCallees,
	} {
		response := m.session.Register(uri, handler).Do()
		if response.Err != nil {
			return response.Err
		}
	}

	return nil
}

func (m *meta) onJoin(base BaseSession) {
	details := map[string]any{
		"session":      base.ID(),
		"authid":       base.AuthID(),
		"authrole":     base.AuthRole(),
		"authmethod":   "",
		"authprovider": "",
	}

	// FIXME: use a goroutine pool
	go func() {
		if m.session != nil {
			m.session.Publish(MetaTopicSessionJoin).Args(details).Do()
		}
	}()
}

func (m *meta) onLeave(base BaseSession) {
	// FIXME: use a goroutine pool
	go func() {
		if m.session != nil {
			m.session.Publish(MetaTopicSessionLeave).Args(base.ID(), base.AuthID(), base.AuthRole()).Do()
		}
	}()
}

func killSession(invocation *Invocation, client BaseSession) error {
	reason := invocation.KwargStringOr("reason", "wamp.close.killed")
	goodByeDetails := map[string]any{}

	if msg, err := invocation.KwargString("message"); err == nil {
		goodByeDetails["message"] = msg
	}

	goodbye := messages.NewGoodBye(reason, goodByeDetails)
	if err := client.WriteMessage(goodbye); err != nil {
		return fmt.Errorf("wamp.error.internal_error")
	}

	_ = client.Close()
	return nil
}

func (m *meta) handleSessionKill(_ context.Context, invocation *Invocation) *InvocationResult {
	sessionID, err := invocation.ArgUInt64(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	if sessionID == m.session.ID() || sessionID == invocation.Caller() {
		return NewInvocationError("wamp.error.no_such_session", "invalid session id")
	}

	rlm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	client, ok := rlm.clients.Load(sessionID)
	if !ok {
		return NewInvocationError("wamp.error.not_found")
	}

	if err := killSession(invocation, client); err != nil {
		return NewInvocationError(err.Error())
	}

	return NewInvocationResult()
}

func (m *meta) handleSessionKillByAuthID(_ context.Context, invocation *Invocation) *InvocationResult {
	authID, err := invocation.ArgString(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	rlm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	sessionIDs := make([]uint64, 0)
	rlm.clients.Range(func(_ uint64, client BaseSession) bool {
		if client.AuthID() == authID && client.ID() != m.session.ID() && client.ID() != invocation.Caller() {
			_ = killSession(invocation, client)
			sessionIDs = append(sessionIDs, client.ID())
		}
		return true
	})

	return NewInvocationResult(sessionIDs)
}

func (m *meta) handleSessionKillByAuthRole(_ context.Context, invocation *Invocation) *InvocationResult {
	authrole, err := invocation.ArgString(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	rlm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	sessionIDs := make([]uint64, 0)
	rlm.clients.Range(func(_ uint64, client BaseSession) bool {
		if client.AuthRole() == authrole && client.ID() != m.session.ID() && client.ID() != invocation.Caller() {
			_ = killSession(invocation, client)
			sessionIDs = append(sessionIDs, client.ID())
		}
		return true
	})

	return NewInvocationResult(sessionIDs)
}

func (m *meta) handleSessionKillAll(_ context.Context, invocation *Invocation) *InvocationResult {
	rlm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	sessionIDs := make([]uint64, 0)
	rlm.clients.Range(func(_ uint64, client BaseSession) bool {
		if client.ID() != m.session.ID() && client.ID() != invocation.Caller() {
			_ = killSession(invocation, client)
			sessionIDs = append(sessionIDs, client.ID())
		}
		return true
	})

	return NewInvocationResult(sessionIDs)
}

func (m *meta) forEachSession(invocation *Invocation, fn func(sess BaseSession)) error {
	var roles []any
	if len(invocation.Args()) > 0 {
		r, err := invocation.ArgList(0)
		if err != nil {
			return fmt.Errorf("wamp.error.invalid_argument")
		}
		roles = r
	}

	rlm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return fmt.Errorf("wamp.error.not_found")
	}

	rlm.clients.Range(func(_ uint64, sess BaseSession) bool {
		if len(roles) == 0 || contains(roles, sess.AuthRole()) {
			fn(sess)
		}
		return true
	})

	return nil
}

func (m *meta) handleSessionCount(_ context.Context, invocation *Invocation) *InvocationResult {
	var count uint64
	err := m.forEachSession(invocation, func(sess BaseSession) {
		count++
	})
	if err != nil {
		return NewInvocationError(err.Error())
	}
	return NewInvocationResult(count)
}

func (m *meta) handleSessionList(_ context.Context, invocation *Invocation) *InvocationResult {
	var sessionIDs []uint64
	err := m.forEachSession(invocation, func(sess BaseSession) {
		sessionIDs = append(sessionIDs, sess.ID())
	})
	if err != nil {
		return NewInvocationError(err.Error())
	}
	return NewInvocationResult(sessionIDs)
}

func (m *meta) handleSessionGet(_ context.Context, invocation *Invocation) *InvocationResult {
	if invocation.ArgsLen() != 1 {
		return NewInvocationError("wamp.error.invalid_argument")
	}

	sessionID, err := invocation.ArgUInt64(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	rlm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	client, ok := rlm.clients.Load(sessionID)
	if !ok {
		return NewInvocationError("wamp.error.no_such_session", "invalid session id")
	}

	details := map[string]any{
		"session":      client.ID(),
		"authid":       client.AuthID(),
		"authrole":     client.AuthRole(),
		"authmethod":   "",
		"authprovider": "",
	}
	return NewInvocationResult(details)
}

func contains(slice []any, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

func (m *meta) OnRegistrationCreated(reg *wampproto.Registration) {
	var calleeSession uint64
	for callee := range reg.Registrants {
		calleeSession = callee
	}
	registrationDetails := map[string]any{
		"id":      reg.ID,
		"created": reg.Created,
		"uri":     reg.Procedure,
		"match":   reg.Match,
		"invoke":  reg.InvocationPolicy,
	}
	// FIXME: use a goroutine pool
	go func() {
		if m.session != nil {
			m.session.Publish(MetaTopicRegistrationCreate).Args(calleeSession, registrationDetails).Do()
		}
	}()
}

func (m *meta) OnRegistrationRegister(reg wampproto.RegistrationEvent) {
	// FIXME: use a goroutine pool
	go func() {
		if m.session != nil {
			m.session.Publish(MetaTopicRegistrationRegister).Args(reg.SessionID, reg.RegistrationID).Do()
		}
	}()
}

func (m *meta) OnRegistrationUnregister(reg wampproto.RegistrationEvent) {
	// FIXME: use a goroutine pool
	go func() {
		if m.session != nil {
			m.session.Publish(MetaTopicRegistrationUnregister).Args(reg.SessionID, reg.RegistrationID).Do()
		}
	}()
}

func (m *meta) OnRegistrationDeleted(reg wampproto.RegistrationEvent) {
	// FIXME: use a goroutine pool
	go func() {
		if m.session != nil {
			m.session.Publish(MetaTopicRegistrationDelete).Args(reg.SessionID, reg.RegistrationID).Do()
		}
	}()
}

func (m *meta) handleRegistrationList(_ context.Context, _ *Invocation) *InvocationResult {
	realm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	registrationList := map[string]any{
		"exact":    maps.Keys(realm.dealer.ExactRegistrationsByID()),
		"prefix":   maps.Keys(realm.dealer.PrefixRegistrationsByID()),
		"wildcard": maps.Keys(realm.dealer.WildCardRegistrationsByID()),
	}

	return NewInvocationResult(registrationList)
}

func (m *meta) handleRegistrationLookup(_ context.Context, invocation *Invocation) *InvocationResult {
	if invocation.ArgsLen() < 1 {
		return NewInvocationError("wamp.error.invalid_argument")
	}

	procedure, err := invocation.ArgString(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	realm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	reg, ok := realm.dealer.RegistrationsByProcedure()[procedure]
	if !ok {
		return NewInvocationResult(nil)
	}

	return NewInvocationResult(reg.ID)
}

func (m *meta) handleRegistrationMatch(_ context.Context, invocation *Invocation) *InvocationResult {
	if invocation.ArgsLen() != 1 {
		return NewInvocationError("wamp.error.invalid_argument")
	}

	procedure, err := invocation.ArgString(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	realm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	reg, ok := realm.dealer.MatchRegistration(procedure)
	if !ok {
		return NewInvocationResult(nil)
	}

	return NewInvocationResult(reg.ID)
}

func (m *meta) handleRegistrationGet(_ context.Context, invocation *Invocation) *InvocationResult {
	if invocation.ArgsLen() != 1 {
		return NewInvocationError("wamp.error.invalid_argument")
	}

	id, err := invocation.ArgUInt64(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	realm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	reg, ok := realm.findRegistrationByID(id)
	if !ok {
		return NewInvocationError("wamp.error.no_such_registration")
	}

	registrationDetails := map[string]any{
		"id":      reg.ID,
		"created": reg.Created,
		"uri":     reg.Procedure,
		"match":   reg.Match,
		"invoke":  reg.InvocationPolicy,
	}

	return NewInvocationResult(registrationDetails)
}

func (m *meta) handleRegistrationListCallees(_ context.Context, invocation *Invocation) *InvocationResult {
	if invocation.ArgsLen() != 1 {
		return NewInvocationError("wamp.error.invalid_argument")
	}

	id, err := invocation.ArgUInt64(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	realm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	reg, ok := realm.findRegistrationByID(id)
	if !ok {
		return NewInvocationError("wamp.error.no_such_registration")
	}

	callees := maps.Keys(reg.Registrants)
	return NewInvocationResult(callees)
}

func (m *meta) handleRegistrationCountCallees(_ context.Context, invocation *Invocation) *InvocationResult {
	if invocation.ArgsLen() != 1 {
		return NewInvocationError("wamp.error.invalid_argument")
	}

	id, err := invocation.ArgUInt64(0)
	if err != nil {
		return NewInvocationError("wamp.error.invalid_argument", err.Error())
	}

	realm, ok := m.router.realms.Load(m.realm)
	if !ok {
		return NewInvocationError("wamp.error.not_found", "invalid realm")
	}

	reg, ok := realm.findRegistrationByID(id)
	if !ok {
		return NewInvocationError("wamp.error.no_such_registration")
	}

	return NewInvocationResult(len(reg.Registrants))
}
