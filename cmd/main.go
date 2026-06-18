// main.go is the operator entrypoint.
// It wires all dependencies, starts the controller-manager, and connects RTDS.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/controller"
	"github.com/chaitanyasrivastav/shedpilot/internal/rtds"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		leaderElect bool
		enableRTDS  bool
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Metrics endpoint address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Health probe endpoint address")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader election (required for 2-replica HA)")
	flag.BoolVar(&enableRTDS, "enable-rtds", true, "Enable RTDS for sub-200ms profile switching")

	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "shedpilot.resilience.shedpilot.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// ── Signal context — created once, reused by RTDS and manager ───────────
	// SetupSignalHandler must only be called once — it registers OS signal
	// handlers and panics if called twice.
	ctx := ctrl.SetupSignalHandler()

	// ── Wire RTDS client ──────────────────────────────────────────────────────
	var rtdsClient *rtds.Client
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		setupLog.Error(err, "unable to get rest config")
		os.Exit(1)
	}
	if enableRTDS {
		client, err := rtds.NewClient(restConfig, mgr.GetClient())
		if err != nil {
			setupLog.Error(err, "unable to create rtds client, falling back to EnvoyFilter path")
		} else {
			rtdsClient = client
			setupLog.Info("fast delivery client ready (Envoy admin API)")
		}
	} else {
		setupLog.Info("RTDS disabled — using EnvoyFilter path only")
	}

	// ── Register controller ───────────────────────────────────────────────────

	reconciler := controller.NewAdaptivePolicyReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		rtdsClient,
	)

	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	// ── Health probes ─────────────────────────────────────────────────────────

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up readyz")
		os.Exit(1)
	}

	// ── Start ─────────────────────────────────────────────────────────────────

	setupLog.Info("starting shedpilot operator",
		"rtdsEnabled", enableRTDS && rtdsClient != nil,
		"leaderElection", leaderElect,
	)

	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
