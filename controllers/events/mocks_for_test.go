package events

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
	tracesdk "go.opentelemetry.io/otel/sdk/export/trace"
	"go.opentelemetry.io/otel/semconv"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicFake "k8s.io/client-go/dynamic/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func newMockRESTMapper() meta.RESTMapper {
	cfg := &rest.Config{}
	mapper, _ := apiutil.NewDynamicRESTMapper(cfg, apiutil.WithCustomMapper(func() (meta.RESTMapper, error) {
		baseMapper := meta.NewDefaultRESTMapper(nil)
		// Add the object kinds that we use in fixtures.
		baseMapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "deployment"}, meta.RESTScopeNamespace)
		baseMapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "replicaset"}, meta.RESTScopeNamespace)
		baseMapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "statefulset"}, meta.RESTScopeNamespace)
		baseMapper.Add(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "pod"}, meta.RESTScopeNamespace)

		return baseMapper, nil
	}))
	return mapper
}

// Initialize an EventWatcher, context and logger ready for testing
func newTestEventWatcher(initObjs ...runtime.Object) (context.Context, *EventWatcher, *fakeExporter, logr.Logger) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	log := zap.New(zap.UseDevMode(true))

	fakeClient := fake.NewFakeClientWithScheme(scheme, initObjs...)
	exporter := newFakeExporter()

	r := &EventWatcher{
		Client:   fakeClient,
		Log:      log,
		Exporter: exporter,
	}

	fakeDynamic := dynamicFake.NewSimpleDynamicClient(scheme)
	mockRESTMapper := newMockRESTMapper()
	r.initialize(fakeDynamic, mockRESTMapper)

	return ctx, r, exporter, log
}

func newFakeExporter() *fakeExporter {
	return &fakeExporter{}
}

// records spans sent to it, for testing purposes
type fakeExporter struct {
	spanData []*tracesdk.SpanData
}

func (f *fakeExporter) reset() {
	f.spanData = nil
}

func (f *fakeExporter) dump() []string {
	spanMap := make(map[trace.SpanID]int)
	for i, d := range f.spanData {
		spanMap[d.SpanContext.SpanID] = i
	}
	var ret []string
	for i, d := range f.spanData {
		parent, found := spanMap[d.ParentSpanID]
		var parentStr string
		if found {
			parentStr = fmt.Sprintf(" (%d)", parent)
		}
		message := labelValue(d.Attributes, label.Key("message"))
		resourceName := labelValue(d.Resource.Attributes(), semconv.ServiceNameKey)
		ret = append(ret, fmt.Sprintf("%d: %s %s%s %s", i, resourceName, d.Name, parentStr, message))
	}
	return ret
}

// ExportSpans implements trace.SpanExporter
func (f *fakeExporter) ExportSpans(ctx context.Context, spanData []*tracesdk.SpanData) error {
	f.spanData = append(f.spanData, spanData...)
	return nil
}

// Shutdown implements trace.SpanExporter
func (f *fakeExporter) Shutdown(ctx context.Context) error {
	return nil
}

func labelValue(labels []label.KeyValue, key label.Key) string {
	for _, lbl := range labels {
		if lbl.Key == key {
			return lbl.Value.AsString()
		}
	}
	return ""
}
