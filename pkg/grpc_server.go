package pkg

import (
	"fmt"
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/logging"
	sentryutils "github.com/catalystsquad/app-utils-go/sentry"
	"github.com/getsentry/sentry-go"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
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
	Port                              int
	SentryEnabled                     bool
	SentryClientOptions               sentry.ClientOptions
	PrometheusEnabled                 bool
	PrometheusPath                    string
	PrometheusPort                    int
	PrometheusEnableLatencyHistograms bool
	GetErrorToReturn                  func(err error) error
	CaptureRecoveredErr               func(err error) bool
	CaptureErrormessage               string
}

func NewGrpcServer(config GrpcServerConfig) *GrpcServer {
	grpcServer := &GrpcServer{
		Config: config,
	}
	grpcServer.initialize()
	return grpcServer
}

func (s *GrpcServer) initialize() {
	opts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			recoveredErr := errorutils.RecoverErr(p)
			err = s.Config.GetErrorToReturn(recoveredErr)
			if s.Config.CaptureRecoveredErr(err) {
				errorutils.LogOnErr(nil, s.Config.CaptureErrormessage, err)
			}
			return
		}),
	}
	// create grpc server
	server := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_prometheus.UnaryServerInterceptor,
				grpc_recovery.UnaryServerInterceptor(opts...),
			),
		),
	)

	// register health service (used in k8s health checks)
	healthService := NewHealthChecker()
	grpc_health_v1.RegisterHealthServer(server, healthService)
	s.Server = server
}

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

func (s *GrpcServer) maybeInitSentry() {
	if s.Config.SentryEnabled {
		sentryutils.MaybeInitSentry(s.Config.SentryClientOptions, nil)
	}
}

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
