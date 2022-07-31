package pkg

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/logging"
	sentryutils "github.com/catalystsquad/app-utils-go/sentry"
	"github.com/getsentry/sentry-go"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // import for side effects, enables clients to use gzip compression
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
)

var shutDown = make(chan struct{})
var runError = make(chan error)
var wg = new(sync.WaitGroup)

type GrpcServer struct {
	Config GrpcServerConfig
	Server *grpc.Server
}

type GrpcServerConfig struct {
	Port                               int                   // port to run on
	SentryEnabled                      bool                  // enable sentry integration
	SentryClientOptions                sentry.ClientOptions  // arbitrary sentry client options to pass through to sentry client
	PrometheusEnabled                  bool                  // enable prometheus metrics
	PrometheusPath                     string                // path to enable prometheus metrics on
	PrometheusPort                     int                   // port to run prometheus metrics on
	PrometheusEnableLatencyHistograms  bool                  // enable prometheus latency histograms
	GetErrorToReturn                   func(err error) error // called when recovering from a panic, gets the error to return to the caller
	CaptureRecoveredErr                func(err error) bool  // called when recovering from a panic, return true to capture the error in sentry
	CaptureErrormessage                string                // error message logged when recovering from a panic
	Opts                               []grpc.ServerOption   // arbitrary options to pass through to the server
	TlsCertPath, TlsKeyPath, TlsCaPath string                // file paths to tls cert, key, and ca, if all 3 are provided then the server runs with tls enabled
	MinTlsVersion                      uint16                // minimum tls version to use, defaults to 1.0
	UnaryServerInterceptors            []grpc.UnaryServerInterceptor
	StreamServerInterceptors           []grpc.StreamServerInterceptor
	AuthFunc                           grpc_auth.AuthFunc
}

// NewGrpcServer instantiates and initializes a new grpc server. It does not run the server.
func NewGrpcServer(config GrpcServerConfig) (*GrpcServer, error) {
	if config.GetErrorToReturn == nil {
		// by default, return an internal server error
		config.GetErrorToReturn = func(err error) error {
			return status.Error(codes.Internal, "unexpected error handling request")
		}
	}
	if config.CaptureRecoveredErr == nil {
		// by default return sentry enabled, which will capture either all or none depending on sentry settings
		config.CaptureRecoveredErr = func(err error) bool {
			return config.SentryEnabled
		}
	}
	grpcServer := &GrpcServer{
		Config: config,
	}
	err := grpcServer.initialize()
	return grpcServer, err
}

// initialize() initializes the server with the config
func (s *GrpcServer) initialize() error {
	unaryInterceptorChain := s.getUnaryInterceptorChain()
	unaryInterceptorOpt := grpc.UnaryInterceptor(
		grpc_middleware.ChainUnaryServer(
			unaryInterceptorChain,
		),
	)
	streamInterceptorChain := s.getStreamInterceptorChain()
	streamInterceptorOpt := grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			streamInterceptorChain,
		),
	)
	s.Config.Opts = append(s.Config.Opts, unaryInterceptorOpt, streamInterceptorOpt)
	err := s.maybeLoadTLSCredentials()
	if err != nil {
		return err
	}
	// create grpc server with options
	server := grpc.NewServer(s.Config.Opts...)

	// register health service (used in k8s health checks)
	healthService := NewHealthChecker()
	grpc_health_v1.RegisterHealthServer(server, healthService)
	s.Server = server
	return nil
}

// Run runs the grpc server, call this after creating a server with NewGrpcServer()
func (s *GrpcServer) Run() (err error) {
	// listen for os signals
	var osSignal = make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	wg.Add(1)
	// run the server
	go s.run()
	// wait for either error or os signal to terminate
	select {
	case runErr := <-runError:
		err = runErr
		errorutils.LogOnErr(nil, "error running gRPC server", err)
	case <-osSignal:
		// nothing special on osSignal, just break the select
	}
	// close shutdown to stop the server
	close(shutDown)
	// wait for shutdown
	wg.Wait()
	return
}

// maybeInitSentry initializes a sentry client if configured to do so
func (s *GrpcServer) maybeInitSentry() {
	if s.Config.SentryEnabled {
		sentryutils.MaybeInitSentry(s.Config.SentryClientOptions, nil)
	}
}

// servePrometheusMetrics serves prometheus metrics
func (s *GrpcServer) servePrometheusMetrics() {
	// register prometheus
	grpc_prometheus.Register(s.Server)
	// Register Prometheus metrics handler.
	http.Handle(s.Config.PrometheusPath, promhttp.Handler())
	// enable latency histograms
	if s.Config.PrometheusEnableLatencyHistograms {
		grpc_prometheus.EnableHandlingTimeHistogram()
	}
	err := http.ListenAndServe(fmt.Sprintf(":%d", s.Config.PrometheusPort), nil)
	errorutils.PanicOnErr(nil, "error serving prometheus metrics", err)
}

//run is the internal run implementation
func (s *GrpcServer) run() {
	defer wg.Done()
	s.maybeInitSentry()
	// create listener
	listenOn := fmt.Sprintf("0.0.0.0:%d", s.Config.Port)
	listener, err := net.Listen("tcp", listenOn)
	errorutils.LogOnErr(nil, "error creating grpc listener", err)

	if s.Config.PrometheusEnabled {
		go s.servePrometheusMetrics()
	}

	// serve
	go func() {
		logging.Log.WithField("listening_on", listenOn).Info("gRPC server started")
		runError <- s.Server.Serve(listener)
	}()

	<-shutDown
	s.Server.Stop()
}

// MaybeLoadTLSCredentials loads TLS transport credentials into the server options if the tls cert path, key path, and
// ca path are specified.
func (s *GrpcServer) maybeLoadTLSCredentials() error {
	if s.Config.TlsCertPath != "" && s.Config.TlsKeyPath != "" && s.Config.TlsCaPath != "" {
		if s.Config.MinTlsVersion == 0 {
			s.Config.MinTlsVersion = tls.VersionTLS10
		}
		logging.Log.WithFields(logrus.Fields{
			"min_tls_version": s.Config.MinTlsVersion,
			"cert_path":       s.Config.TlsCertPath,
			"key_path":        s.Config.TlsKeyPath,
			"ca_path":         s.Config.TlsCaPath,
		}).Info("running with tls enabled")
		srv, err := tls.LoadX509KeyPair(s.Config.TlsCertPath, s.Config.TlsKeyPath)
		if err != nil {
			return err
		}

		p := x509.NewCertPool()

		if s.Config.TlsCaPath != "" {
			ca, err := ioutil.ReadFile(s.Config.TlsCaPath)
			if err != nil {
				return err
			}

			p.AppendCertsFromPEM(ca)
		}
		creds := grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:   s.Config.MinTlsVersion,
			Certificates: []tls.Certificate{srv},
			RootCAs:      p,
		}))

		s.Config.Opts = append(s.Config.Opts, creds)
	}
	return nil
}

func (s *GrpcServer) getUnaryInterceptorChain() grpc.UnaryServerInterceptor {
	recoverOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			recoveredErr := errorutils.RecoverErr(p)
			err = s.Config.GetErrorToReturn(recoveredErr)
			if s.Config.CaptureRecoveredErr(err) {
				errorutils.LogOnErr(nil, s.Config.CaptureErrormessage, err)
			}
			return
		}),
	}
	// add default interceptors
	interceptorChain := grpc_middleware.ChainUnaryServer(
		grpc_prometheus.UnaryServerInterceptor,
		grpc_recovery.UnaryServerInterceptor(recoverOpts...),
	)
	// add auth interceptor if we need to
	if s.Config.AuthFunc != nil {
		interceptorChain = grpc_middleware.ChainUnaryServer(
			interceptorChain,
			grpc_auth.UnaryServerInterceptor(s.Config.AuthFunc),
		)
	}
	// add any additional interceptors
	for _, interceptor := range s.Config.UnaryServerInterceptors {
		interceptorChain = grpc_middleware.ChainUnaryServer(
			interceptorChain,
			interceptor,
		)
	}
	return interceptorChain
}

func (s *GrpcServer) getStreamInterceptorChain() grpc.StreamServerInterceptor {
	recoverOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			recoveredErr := errorutils.RecoverErr(p)
			err = s.Config.GetErrorToReturn(recoveredErr)
			if s.Config.CaptureRecoveredErr(err) {
				errorutils.LogOnErr(nil, s.Config.CaptureErrormessage, err)
			}
			return
		}),
	}
	// add default interceptors
	interceptorChain := grpc_middleware.ChainStreamServer(
		grpc_prometheus.StreamServerInterceptor,
		grpc_recovery.StreamServerInterceptor(recoverOpts...),
	)
	// add auth interceptor if we need to
	if s.Config.AuthFunc != nil {
		interceptorChain = grpc_middleware.ChainStreamServer(
			interceptorChain,
			grpc_auth.StreamServerInterceptor(s.Config.AuthFunc),
		)
	}
	// add any additional interceptors
	for _, interceptor := range s.Config.StreamServerInterceptors {
		interceptorChain = grpc_middleware.ChainStreamServer(
			interceptorChain,
			interceptor,
		)
	}
	return interceptorChain
}
