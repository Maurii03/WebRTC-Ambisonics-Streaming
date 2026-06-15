package codec

// PCM conversion helpers crossing the float32 (client / WebCodecs) ↔ int16
// (libopus) boundary. The browser captures and plays "f32-planar" samples in
// [-1, 1]; libopus encodes/decodes int16. These two functions are the only
// place the MCU converts between the two representations.
//
// Scaling convention: +1.0 maps to +32767 and −1.0 to −32768. We multiply by
// 32767 and clamp to the int16 range so out-of-range floats saturate instead of
// wrapping. Conversion is intentionally truncating (no dithering) — adequate for
// transport; a single-LSB difference on round-trip is expected.

const (
	floatToInt16Scale = 32767.0
	int16ToFloatScale = 1.0 / 32768.0

	int16Max = 32767
	int16Min = -32768
)

// Float32ToInt16 converts src (float32 in [-1, 1]) into dst (int16), clamping
// out-of-range values. It converts min(len(dst), len(src)) samples.
func Float32ToInt16(dst []int16, src []float32) {
	n := len(src)
	if len(dst) < n {
		n = len(dst)
	}
	for i := 0; i < n; i++ {
		v := src[i] * floatToInt16Scale
		switch {
		case v > int16Max:
			dst[i] = int16Max
		case v < int16Min:
			dst[i] = int16Min
		default:
			dst[i] = int16(v)
		}
	}
}

// Int16ToFloat32 converts src (int16) into dst (float32 in [-1, 1)). It converts
// min(len(dst), len(src)) samples.
func Int16ToFloat32(dst []float32, src []int16) {
	n := len(src)
	if len(dst) < n {
		n = len(dst)
	}
	for i := 0; i < n; i++ {
		dst[i] = float32(src[i]) * int16ToFloatScale
	}
}
