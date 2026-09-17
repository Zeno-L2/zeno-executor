// Package batch is the STF driver and batch builder: the execution loop that decodes and
// applies a domain's input stream, computes pre/post state roots and witnesses, slices the
// stream into contiguous batches under the §25.1/§27.9 caps, and assembles each batch's
// commitment and DA bundle, self-verified through a round-trip before NextBatch returns it.
package batch
