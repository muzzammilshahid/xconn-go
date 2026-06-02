package xconn_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-go"
)

func startYamuxRouter(t *testing.T) (addr string, server *xconn.Server) {
	t.Helper()

	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	err = router.AddRealm(realmName, xconn.DefaultRealmConfig())
	require.NoError(t, err)
	err = router.AddRealmRole(realmName, xconn.RealmRole{
		Name: roleAnonymous,
		Permissions: []xconn.Permission{{
			URI:            "",
			MatchPolicy:    matchPrefix,
			AllowCall:      true,
			AllowRegister:  true,
			AllowPublish:   true,
			AllowSubscribe: true,
		}},
	})
	require.NoError(t, err)

	server = xconn.NewServer(router, nil, nil)

	listener, err := server.ListenAndServeYamux(xconn.NetworkTCP, "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	return listener.Addr().String(), server
}

func TestYamuxWAMPSession(t *testing.T) {
	addr, _ := startYamuxRouter(t)

	sess, err := xconn.DialYamux(context.Background(), addr, realmName, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	require.True(t, sess.Connected())

	regResp := sess.Register("io.xconn.yamux.echo",
		func(ctx context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
			return xconn.NewInvocationResult(inv.Args()...)
		}).Do()
	require.NoError(t, regResp.Err)

	callResp := sess.Call("io.xconn.yamux.echo").Args("hello", "yamux").Do()
	require.NoError(t, callResp.Err)
	require.Equal(t, "hello", callResp.ArgStringOr(0, ""))
	require.Equal(t, "yamux", callResp.ArgStringOr(1, ""))
}

func TestYamuxRawStream(t *testing.T) {
	addr, server := startYamuxRouter(t)

	streamData := make(chan []byte, 1)
	server.SetStreamHandler(func(_ xconn.BaseSession, stream net.Conn) {
		buf, err := io.ReadAll(stream)
		if err == nil {
			streamData <- buf
		}
		_ = stream.Close()
	})

	sess, err := xconn.DialYamux(context.Background(), addr, realmName, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	stream, err := sess.OpenStream()
	require.NoError(t, err)

	_, err = stream.Write([]byte("raw file data"))
	require.NoError(t, err)
	_ = stream.Close()

	received := <-streamData
	require.Equal(t, "raw file data", string(received))
}

func TestYamuxConnHandler(t *testing.T) {
	addr, server := startYamuxRouter(t)

	// Server opens a stream to the client and writes a greeting.
	server.SetYamuxConnHandler(func(_ context.Context, _ xconn.BaseSession, conn *xconn.YamuxClientConn) {
		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		_, _ = stream.Write([]byte("hello from server"))
		_ = stream.Close()
	})

	sess, err := xconn.DialYamux(context.Background(), addr, realmName, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	// Client accepts the server-initiated stream and reads the greeting.
	stream, err := sess.AcceptStream()
	require.NoError(t, err)
	defer stream.Close()

	buf := make([]byte, 32)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "hello from server", string(buf[:n]))
}

func TestYamuxStreamHasAuthenticatedSession(t *testing.T) {
	addr, server := startYamuxRouter(t)

	sessionIDCh := make(chan uint64, 1)
	server.SetStreamHandler(func(session xconn.BaseSession, stream net.Conn) {
		sessionIDCh <- session.ID()
		_ = stream.Close()
	})

	sess, err := xconn.DialYamux(context.Background(), addr, realmName, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	stream, err := sess.OpenStream()
	require.NoError(t, err)
	_ = stream.Close()

	serverSessionID := <-sessionIDCh
	require.NotZero(t, serverSessionID)
}
