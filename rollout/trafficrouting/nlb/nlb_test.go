package nlb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/aws"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	ingressutil "github.com/argoproj/argo-rollouts/utils/ingress"
	"github.com/argoproj/argo-rollouts/utils/record"
)

type fakeAWS struct {
	lb *elbv2types.LoadBalancer
	tg []aws.TargetGroupMeta
}

func (f *fakeAWS) GetTargetGroupHealth(ctx context.Context, targetGroupARN string) ([]elbv2types.TargetHealthDescription, error) {
	return nil, nil
}

func (f *fakeAWS) GetTargetGroupMetadata(ctx context.Context, loadBalancerARN string) ([]aws.TargetGroupMeta, error) {
	return f.tg, nil
}

func (f *fakeAWS) FindLoadBalancerByDNSName(ctx context.Context, dnsName string) (*elbv2types.LoadBalancer, error) {
	return f.lb, nil
}

func int32p(i int32) *int32 {
	return &i
}

func newRolloutForTest() *v1alpha1.Rollout {
	return &v1alpha1.Rollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: v1alpha1.RolloutSpec{
			Strategy: v1alpha1.RolloutStrategy{
				Canary: &v1alpha1.CanaryStrategy{
					CanaryService: "canary-svc",
					StableService: "stable-svc",
					TrafficRouting: &v1alpha1.RolloutTrafficRouting{
						NLB: &v1alpha1.NLBTrafficRouting{
							Service:  "ingress-svc",
							Port:     80,
							Protocol: "TCP",
						},
					},
					Steps: []v1alpha1.CanaryStep{
						{SetWeight: int32p(20)},
					},
				},
			},
		},
		Status: v1alpha1.RolloutStatus{
			StableRS:           "stable-hash",
			CurrentStepIndex:   int32p(0),
			CurrentPodHash:     "podhash",
			PromoteFull:        false,
			Canary:             v1alpha1.CanaryStatus{},
			ObservedGeneration: 1,
		},
	}
}

func TestSetWeightPatchesServiceAnnotations(t *testing.T) {
	ro := newRolloutForTest()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ingress-svc",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{Hostname: "lb.example.com"}},
			},
		},
	}

	stableID := aws.BuildTargetGroupResourceID("default", "ingress-svc", "stable-svc", 80)
	canaryID := aws.BuildTargetGroupResourceID("default", "ingress-svc", "canary-svc", 80)
	fakeClient := &fakeAWS{
		lb: &elbv2types.LoadBalancer{
			LoadBalancerArn:  ptr.To("arn:aws:elasticloadbalancing:123"),
			LoadBalancerName: ptr.To("nlb"),
			DNSName:          ptr.To("lb.example.com"),
		},
		tg: []aws.TargetGroupMeta{
			{
				TargetGroup: elbv2types.TargetGroup{
					TargetGroupArn: ptr.To("arn:tg:canary"),
				},
				Tags: map[string]string{defaults.GetalbTagKeyResourceID(): canaryID},
			},
			{
				TargetGroup: elbv2types.TargetGroup{
					TargetGroupArn: ptr.To("arn:tg:stable"),
				},
				Tags: map[string]string{defaults.GetalbTagKeyResourceID(): stableID},
			},
		},
	}

	client := fake.NewSimpleClientset(svc)
	r, err := NewReconciler(ReconcilerConfig{
		Rollout:  ro,
		Client:   client,
		Recorder: record.NewFakeEventRecorder(),
		Status:   &ro.Status,
	})
	assert.NoError(t, err)
	r.aws = fakeClient

	err = r.SetWeight(20)
	assert.NoError(t, err)

	updatedSvc, err := client.CoreV1().Services("default").Get(context.Background(), "ingress-svc", metav1.GetOptions{})
	assert.NoError(t, err)

	actionKey := serviceActionAnnotationKey("TCP", 80)
	val, ok := updatedSvc.Annotations[actionKey]
	assert.True(t, ok, "expected action annotation")

	var action nlbAction
	err = json.Unmarshal([]byte(val), &action)
	assert.NoError(t, err)
	if assert.Len(t, action.ForwardConfig.TargetGroups, 2) {
		assert.Equal(t, "arn:tg:canary", action.ForwardConfig.TargetGroups[0].TargetGroupArn)
		assert.Equal(t, int64(20), *action.ForwardConfig.TargetGroups[0].Weight)
		assert.Equal(t, "arn:tg:stable", action.ForwardConfig.TargetGroups[1].TargetGroupArn)
		assert.Equal(t, int64(80), *action.ForwardConfig.TargetGroups[1].Weight)
	}

	m, err := ingressutil.NewManagedALBAnnotations(updatedSvc.Annotations[managedAnnotationsKey])
	assert.NoError(t, err)
	assert.Contains(t, m["demo"], actionKey)
}

func TestVerifyWeightMatchesTargetGroups(t *testing.T) {
	ro := newRolloutForTest()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ingress-svc",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{Hostname: "lb.example.com"}},
			},
		},
	}

	stableID := aws.BuildTargetGroupResourceID("default", "ingress-svc", "stable-svc", 80)
	canaryID := aws.BuildTargetGroupResourceID("default", "ingress-svc", "canary-svc", 80)

	fakeClient := &fakeAWS{
		lb: &elbv2types.LoadBalancer{
			LoadBalancerArn:  ptr.To("arn:aws:elasticloadbalancing:123"),
			LoadBalancerName: ptr.To("nlb"),
			DNSName:          ptr.To("lb.example.com"),
		},
		tg: []aws.TargetGroupMeta{
			{
				TargetGroup: elbv2types.TargetGroup{
					TargetGroupArn: ptr.To("arn:tg:canary"),
				},
				Tags:   map[string]string{defaults.GetalbTagKeyResourceID(): canaryID},
				Weight: int32p(20),
			},
			{
				TargetGroup: elbv2types.TargetGroup{
					TargetGroupArn: ptr.To("arn:tg:stable"),
				},
				Tags:   map[string]string{defaults.GetalbTagKeyResourceID(): stableID},
				Weight: int32p(80),
			},
		},
	}

	client := fake.NewSimpleClientset(svc)
	recorder := record.NewFakeEventRecorder()
	r, err := NewReconciler(ReconcilerConfig{
		Rollout:  ro,
		Client:   client,
		Recorder: recorder,
		Status:   &ro.Status,
	})
	assert.NoError(t, err)
	r.aws = fakeClient

	verified, err := r.VerifyWeight(20)
	assert.NoError(t, err)
	assert.NotNil(t, verified)
	assert.True(t, *verified)
}
