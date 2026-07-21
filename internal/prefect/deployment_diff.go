package prefect

import "reflect"

// DeploymentUpToDate reports whether the remote deployment already matches
// the desired spec for every field the operator manages, so the update call
// can be skipped entirely. This matters because the Prefect server deletes
// all future auto-scheduled runs on EVERY deployment update — even one that
// changes nothing — which shrinks the scheduling horizon and can drop runs.
//
// The comparison is desired-driven: a field the spec omits is not sent in
// the PATCH (omitempty client-side, exclude_unset server-side), so it cannot
// change the remote deployment and is ignored here. Schedules are managed
// separately by the controller and are not compared.
func DeploymentUpToDate(remote *Deployment, desired *DeploymentSpec) bool {
	if remote == nil || desired == nil {
		return false
	}
	if remote.Name != desired.Name || remote.FlowID != desired.FlowID {
		return false
	}
	if !ptrMatches(desired.Description, remote.Description) {
		return false
	}
	if !ptrMatches(desired.Version, remote.Version) {
		return false
	}
	if !ptrMatches(desired.WorkPoolName, remote.WorkPoolName) {
		return false
	}
	if !ptrMatches(desired.WorkQueueName, remote.WorkQueueName) {
		return false
	}
	if !ptrMatches(desired.Entrypoint, remote.Entrypoint) {
		return false
	}
	if !ptrMatches(desired.Path, remote.Path) {
		return false
	}
	if desired.Paused != nil && *desired.Paused != remote.Paused {
		return false
	}
	if desired.EnforceParameterSchema != nil && *desired.EnforceParameterSchema != remote.EnforceParameterSchema {
		return false
	}
	if !concurrencyLimitMatches(remote, desired.ConcurrencyLimit) {
		return false
	}
	// Order-insensitive: tag order is not meaningful and must not force updates.
	if len(desired.Tags) > 0 && !sameStringSet(desired.Tags, remote.Tags) {
		return false
	}
	// desired and remote maps both come from encoding/json unmarshalling
	// (numbers are float64 on both sides), so DeepEqual compares cleanly.
	if desired.Parameters != nil && !reflect.DeepEqual(desired.Parameters, remote.Parameters) {
		return false
	}
	if desired.JobVariables != nil && !reflect.DeepEqual(desired.JobVariables, remote.JobVariables) {
		return false
	}
	if desired.ParameterOpenAPISchema != nil && !reflect.DeepEqual(desired.ParameterOpenAPISchema, remote.ParameterOpenAPISchema) {
		return false
	}
	if desired.PullSteps != nil && !reflect.DeepEqual(desired.PullSteps, remote.PullSteps) {
		return false
	}
	return true
}

// ptrMatches treats a nil desired value as "not managed" (the field is
// omitted from the PATCH payload, so the remote value is left alone).
func ptrMatches[T comparable](desired, remote *T) bool {
	if desired == nil {
		return true
	}
	return remote != nil && *remote == *desired
}

// concurrencyLimitMatches compares the desired concurrency limit against the
// remote deployment. The API's top-level concurrency_limit response field is
// deprecated and always null; the live value is global_concurrency_limit.limit.
func concurrencyLimitMatches(remote *Deployment, desired *int) bool {
	if desired == nil {
		return true
	}
	if remote.GlobalConcurrencyLimit != nil {
		return remote.GlobalConcurrencyLimit.Limit == *desired
	}
	return remote.ConcurrencyLimit != nil && *remote.ConcurrencyLimit == *desired
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}
	return true
}
