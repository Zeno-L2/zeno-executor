// Package batch will hold the STF driver and batch builder: the execution loop that feeds
// decoded inputs to a domain's Apply, computes pre/post state roots, slices a domain's
// input subsequence into contiguous batches, and assembles the batch commitment and DA
// bundle.
package batch
