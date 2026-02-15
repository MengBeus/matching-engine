package symbolspec

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseScaledInt parses a positive decimal string into a fixed-scale int64.
// Example: value=12.34, scale=4 => 123400.
func ParseScaledInt(value string, scale int) (int64, error) {
	s := strings.TrimSpace(value)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}
	if strings.HasPrefix(s, "+") {
		s = strings.TrimPrefix(s, "+")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("value must be positive")
	}

	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return 0, fmt.Errorf("invalid decimal format")
	}

	intPart := parts[0]
	if intPart == "" {
		intPart = "0"
	}
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	if fracPart == "" && len(parts) == 2 {
		return 0, fmt.Errorf("invalid decimal format")
	}
	if len(fracPart) > scale {
		return 0, fmt.Errorf("too many decimal places: max %d", scale)
	}

	for _, ch := range intPart {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid integer digits")
		}
	}
	for _, ch := range fracPart {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid fractional digits")
		}
	}

	scalePow, err := Pow10(scale)
	if err != nil {
		return 0, err
	}

	intVal, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0, err
	}
	if intVal > (1<<63-1)/scalePow {
		return 0, fmt.Errorf("value overflow")
	}
	scaled := intVal * scalePow

	if len(fracPart) > 0 {
		paddedFrac := fracPart + strings.Repeat("0", scale-len(fracPart))
		fracVal, err := strconv.ParseInt(paddedFrac, 10, 64)
		if err != nil {
			return 0, err
		}
		if scaled > (1<<63-1)-fracVal {
			return 0, fmt.Errorf("value overflow")
		}
		scaled += fracVal
	}

	if scaled <= 0 {
		return 0, fmt.Errorf("value must be positive")
	}
	return scaled, nil
}

// FormatScaledInt formats a scaled int64 to decimal and trims trailing zeros.
func FormatScaledInt(v int64, scale int) string {
	if scale <= 0 {
		return strconv.FormatInt(v, 10)
	}
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	scalePow, _ := Pow10(scale)
	intPart := v / scalePow
	fracPart := v % scalePow
	if fracPart == 0 {
		return sign + strconv.FormatInt(intPart, 10)
	}
	frac := fmt.Sprintf("%0*d", scale, fracPart)
	frac = strings.TrimRight(frac, "0")
	return sign + strconv.FormatInt(intPart, 10) + "." + frac
}
