package nlb

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting"
	"github.com/argoproj/argo-rollouts/utils/aws"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	ingressutil "github.com/argoproj/argo-rollouts/utils/ingress"
	jsonutil "github.com/argoproj/argo-rollouts/utils/json"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/record"
	rolloututil "github.com/argoproj/argo-rollouts/utils/rollout"
	"github.com/argoproj/argo-rollouts/utils/weightutil"
)

const (
	// Type holds this controller type
	Type = "NLB"
	// managedAnnotationsKey tracks which annotations are managed by rollouts on the Service
	managedAnnotationsKey = "rollouts.argoproj.io/managed-nlb-annotations"
)

// ReconcilerConfig describes static configuration data for the NLB Service reconciler
type ReconcilerConfig struct {
	Rollout  *v1alpha1.Rollout
	Client   kubernetes.Interface
	Recorder record.EventRecorder
	Status   *v1alpha1.RolloutStatus
}

// Reconciler holds required fields to reconcile NLB Service resources
type Reconciler struct {
	cfg ReconcilerConfig
	log *logrus.Entry
	aws aws.Client
}

// NewReconciler returns a reconciler struct that brings the NLB Service into the desired state
func NewReconciler(cfg ReconcilerConfig) (*Reconciler, error) {
	awsClient, err := aws.NewClient()
	if err != nil {
		return nil, err
	}
	r := Reconciler{
		cfg: cfg,
		log: logutil.WithRollout(cfg.Rollout).WithField(logutil.ServiceKey, cfg.Rollout.Spec.Strategy.Canary.TrafficRouting.NLB.Service),
		aws: awsClient,
	}
	return &r, nil
}

// Type indicates this reconciler is an NLB traffic reconciler
func (r *Reconciler) Type() string {
	return Type
}

// SetWeight adjusts the NLB listener forward action to split traffic between stable/canary (and additional) target groups
func (r *Reconciler) SetWeight(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
	ro := r.cfg.Rollout
	nlbCfg := ro.Spec.Strategy.Canary.TrafficRouting.NLB
	if nlbCfg == nil {
		return nil
	}
	if nlbCfg.Service == "" {
		return fmt.Errorf("nlb.service must be specified")
	}
	listenerPort := nlbCfg.Port
	if listenerPort == 0 {
		return fmt.Errorf("nlb.port must be specified")
	}
	protocol := nlbCfg.Protocol
	if protocol == "" {
		protocol = "TCP"
	}
	rootService := nlbCfg.Service
	if nlbCfg.RootService != "" {
		rootService = nlbCfg.RootService
	}

	ctx := context.TODO()
	svc, err := r.cfg.Client.CoreV1().Services(ro.Namespace).Get(ctx, rootService, metav1.GetOptions{})
	if err != nil {
		return err
	}

	actionValue, err := r.buildForwardAction(ctx, svc, listenerPort, desiredWeight, additionalDestinations...)
	if err != nil {
		return err
	}

	actionKey := serviceActionAnnotationKey(protocol, listenerPort)
	annotations := svc.DeepCopy().GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	desiredAnnotations, err := modifyManagedAnnotation(annotations, ro.Name, true, actionKey)
	if err != nil {
		return err
	}
	if desiredAnnotations[actionKey] == actionValue && svc.Annotations != nil && svc.Annotations[actionKey] == actionValue {
		r.log.Info("no changes to the NLB Service")
		return nil
	}
	if desiredAnnotations == nil {
		desiredAnnotations = make(map[string]string)
	}
	desiredAnnotations[actionKey] = actionValue

	patchBytes, err := buildServiceAnnotationPatch(desiredAnnotations)
	if err != nil {
		return err
	}
	r.log.WithField("patch", string(patchBytes)).Debug("applying NLB Service patch")
	r.log.WithField("desiredWeight", desiredWeight).Info("updating NLB Service annotations")
	r.cfg.Recorder.Eventf(ro, record.EventOptions{EventReason: "PatchingNLBService"}, "Updating Service `%s` to desiredWeight '%d'", rootService, desiredWeight)

	_, err = r.cfg.Client.CoreV1().Services(svc.Namespace).Patch(ctx, svc.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		r.log.WithField("err", err.Error()).Error("error patching nlb service")
		return fmt.Errorf("error patching nlb service `%s`: %v", svc.Name, err)
	}
	return nil
}

func (r *Reconciler) buildForwardAction(ctx context.Context, svc *corev1.Service, port int32, desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) (string, error) {
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return "", fmt.Errorf("service %s has no load balancer ingress assigned", svc.Name)
	}

	lbHostname := svc.Status.LoadBalancer.Ingress[0].Hostname
	if lbHostname == "" && svc.Status.LoadBalancer.Ingress[0].IP != "" {
		lbHostname = svc.Status.LoadBalancer.Ingress[0].IP
	}
	if lbHostname == "" {
		return "", fmt.Errorf("service %s has no load balancer hostname", svc.Name)
	}

	lb, err := r.aws.FindLoadBalancerByDNSName(ctx, lbHostname)
	if err != nil {
		return "", err
	}
	if lb == nil || lb.LoadBalancerArn == nil {
		return "", fmt.Errorf("unable to resolve load balancer for %s", lbHostname)
	}

	tgMeta, err := r.aws.GetTargetGroupMetadata(ctx, *lb.LoadBalancerArn)
	if err != nil {
		return "", err
	}
	resourceIDToARN := map[string]string{}
	for _, tg := range tgMeta {
		if resID, ok := tg.Tags[defaults.GetalbTagKeyResourceID()]; ok {
			resourceIDToARN[resID] = *tg.TargetGroupArn
		}
	}

	stableService, canaryService := trafficrouting.GetStableAndCanaryServices(r.cfg.Rollout, true)
	stableID := aws.BuildTargetGroupResourceID(svc.Namespace, svc.Name, stableService, port)
	canaryID := aws.BuildTargetGroupResourceID(svc.Namespace, svc.Name, canaryService, port)

	targetGroups := make([]nlbTargetGroup, 0)
	if canaryArn, ok := resourceIDToARN[canaryID]; ok {
		targetGroups = append(targetGroups, nlbTargetGroup{TargetGroupArn: canaryArn, Weight: ptr.To[int64](int64(desiredWeight))})
	} else {
		return "", fmt.Errorf("unable to find canary target group for resource id %s", canaryID)
	}
	stableWeight := weightutil.MaxTrafficWeight(r.cfg.Rollout) - desiredWeight

	for _, dest := range additionalDestinations {
		destID := aws.BuildTargetGroupResourceID(svc.Namespace, svc.Name, dest.ServiceName, port)
		arn, ok := resourceIDToARN[destID]
		if !ok {
			return "", fmt.Errorf("unable to find target group for additional destination %s", destID)
		}
		targetGroups = append(targetGroups, nlbTargetGroup{TargetGroupArn: arn, Weight: ptr.To[int64](int64(dest.Weight))})
		stableWeight -= dest.Weight
	}

	if stableArn, ok := resourceIDToARN[stableID]; ok {
		targetGroups = append(targetGroups, nlbTargetGroup{TargetGroupArn: stableArn, Weight: ptr.To[int64](int64(stableWeight))})
	} else {
		return "", fmt.Errorf("unable to find stable target group for resource id %s", stableID)
	}

	action := nlbAction{
		Type: "forward",
		ForwardConfig: nlbForwardConfig{
			TargetGroups: targetGroups,
		},
	}
	bytes := jsonutil.MustMarshal(action)
	return string(bytes), nil
}

func (r *Reconciler) VerifyWeight(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) (*bool, error) {
	if !defaults.VerifyTargetGroup() {
		return nil, nil
	}
	nlbCfg := r.cfg.Rollout.Spec.Strategy.Canary.TrafficRouting.NLB
	if nlbCfg == nil || nlbCfg.Service == "" {
		return nil, nil
	}
	if !rolloututil.ShouldVerifyWeight(r.cfg.Rollout, desiredWeight) {
		return nil, nil
	}

	ctx := context.TODO()
	svc, err := r.cfg.Client.CoreV1().Services(r.cfg.Rollout.Namespace).Get(ctx, nlbCfg.Service, metav1.GetOptions{})
	if err != nil {
		return ptr.To[bool](false), err
	}
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return ptr.To[bool](false), fmt.Errorf("service %s has no load balancer ingress assigned", svc.Name)
	}

	lbHostname := svc.Status.LoadBalancer.Ingress[0].Hostname
	if lbHostname == "" && svc.Status.LoadBalancer.Ingress[0].IP != "" {
		lbHostname = svc.Status.LoadBalancer.Ingress[0].IP
	}
	if lbHostname == "" {
		return ptr.To[bool](false), fmt.Errorf("service %s has no load balancer hostname", svc.Name)
	}

	lb, err := r.aws.FindLoadBalancerByDNSName(ctx, lbHostname)
	if err != nil {
		return ptr.To[bool](false), err
	}
	if lb == nil || lb.LoadBalancerArn == nil {
		return ptr.To[bool](false), fmt.Errorf("unable to resolve load balancer for %s", lbHostname)
	}

	tgMeta, err := r.aws.GetTargetGroupMetadata(ctx, *lb.LoadBalancerArn)
	if err != nil {
		return ptr.To[bool](false), err
	}

	stableService, canaryService := trafficrouting.GetStableAndCanaryServices(r.cfg.Rollout, true)
	port := nlbCfg.Port
	targetsExpected := map[string]int32{}
	targetsExpected[aws.BuildTargetGroupResourceID(svc.Namespace, svc.Name, canaryService, port)] = desiredWeight
	targetsExpected[aws.BuildTargetGroupResourceID(svc.Namespace, svc.Name, stableService, port)] = weightutil.MaxTrafficWeight(r.cfg.Rollout) - desiredWeight
	for _, dest := range additionalDestinations {
		targetsExpected[aws.BuildTargetGroupResourceID(svc.Namespace, svc.Name, dest.ServiceName, port)] = dest.Weight
	}

	verified := int32(0)
	for _, tg := range tgMeta {
		resourceID, ok := tg.Tags[defaults.GetalbTagKeyResourceID()]
		if !ok {
			continue
		}
		expectedWeight, ok := targetsExpected[resourceID]
		if !ok || tg.Weight == nil {
			continue
		}
		if *tg.Weight == expectedWeight {
			verified++
		} else {
			r.log.Infof("target group %s weight %d does not match desired %d", resourceID, *tg.Weight, expectedWeight)
		}
	}

	if verified == int32(len(targetsExpected)) {
		return ptr.To[bool](true), nil
	}
	return ptr.To[bool](false), nil
}

func (r *Reconciler) SetHeaderRoute(headerRoute *v1alpha1.SetHeaderRoute) error {
	return nil
}

func (r *Reconciler) SetMirrorRoute(setMirrorRoute *v1alpha1.SetMirrorRoute) error {
	return nil
}

func (r *Reconciler) UpdateHash(canaryHash, stableHash string, additionalDestinations ...v1alpha1.WeightDestination) error {
	return nil
}

func (r *Reconciler) RemoveManagedRoutes() error {
	nlbCfg := r.cfg.Rollout.Spec.Strategy.Canary.TrafficRouting.NLB
	if nlbCfg == nil || nlbCfg.Service == "" {
		return nil
	}

	actionKey := serviceActionAnnotationKey(nlbCfg.Protocol, nlbCfg.Port)
	desiredAnnotations, err := modifyManagedAnnotation(map[string]string{}, r.cfg.Rollout.Name, false, actionKey)
	if err != nil {
		return err
	}
	patchAnnotations := map[string]interface{}{
		actionKey:             nil,
		managedAnnotationsKey: desiredAnnotations[managedAnnotationsKey],
	}
	patchBytes := jsonutil.MustMarshal(map[string]any{
		"metadata": map[string]any{
			"annotations": patchAnnotations,
		},
	})
	_, err = r.cfg.Client.CoreV1().Services(r.cfg.Rollout.Namespace).Patch(context.TODO(), nlbCfg.Service, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

func serviceActionAnnotationKey(protocol string, port int32) string {
	if protocol == "" {
		protocol = "TCP"
	}
	return fmt.Sprintf("service.beta.kubernetes.io/aws-load-balancer-actions.%s-%d", strings.ToLower(protocol), port)
}

func modifyManagedAnnotation(annotations map[string]string, rolloutName string, add bool, annotationKeys ...string) (map[string]string, error) {
	m, err := ingressutil.NewManagedALBAnnotations(annotations[managedAnnotationsKey])
	if err != nil {
		return nil, err
	}
	managedAnnotation := m[rolloutName]
	if managedAnnotation == nil {
		managedAnnotation = ingressutil.ManagedALBAnnotation{}
	}
	for _, annotationKey := range annotationKeys {
		if add {
			if !hasValue(managedAnnotation, annotationKey) {
				managedAnnotation = append(managedAnnotation, annotationKey)
			}
		} else {
			managedAnnotation = removeValue(managedAnnotation, annotationKey)
		}
	}
	m[rolloutName] = managedAnnotation
	annotations[managedAnnotationsKey] = m.String()
	return annotations, nil
}

func hasValue(array []string, key string) bool {
	for _, item := range array {
		if item == key {
			return true
		}
	}
	return false
}

func removeValue(array []string, key string) []string {
	for i, v := range array {
		if v == key {
			array = append(array[:i], array[i+1:]...)
		}
	}
	return array
}

func buildServiceAnnotationPatch(annotations map[string]string) ([]byte, error) {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
		},
	}
	return jsonutil.MustMarshal(patch), nil
}

// nlbAction defines the forward action for NLB listener
type nlbAction struct {
	Type          string           `json:"type"`
	ForwardConfig nlbForwardConfig `json:"forwardConfig"`
}

type nlbForwardConfig struct {
	TargetGroups []nlbTargetGroup `json:"targetGroups"`
}

type nlbTargetGroup struct {
	TargetGroupArn string `json:"targetGroupArn,omitempty"`
	Weight         *int64 `json:"weight,omitempty"`
}
