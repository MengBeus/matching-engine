package symbolspec

import (
	"fmt"
	"strings"
)

// Spec defines precision and step constraints for a trading symbol.
type Spec struct {
	Symbol        string
	PriceScale    int
	QuantityScale int
	PriceTickInt  int64
	QtyStepInt    int64
}

var specs = map[string]Spec{
	"BTC-USDT": {
		Symbol:        "BTC-USDT",
		PriceScale:    6,
		QuantityScale: 6,
		PriceTickInt:  1,
		QtyStepInt:    1,
	},
	"ETH-USDT": {
		Symbol:        "ETH-USDT",
		PriceScale:    6,
		QuantityScale: 6,
		PriceTickInt:  1,
		QtyStepInt:    1,
	},
	"SOL-USDT": {
		Symbol:        "SOL-USDT",
		PriceScale:    6,
		QuantityScale: 6,
		PriceTickInt:  1,
		QtyStepInt:    1,
	},
}

// Get returns the symbol spec.
func Get(symbol string) (Spec, error) {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	spec, ok := specs[s]
	if !ok {
		return Spec{}, fmt.Errorf("unsupported symbol: %s", symbol)
	}
	return spec, nil
}

// Pow10 returns 10^scale for non-negative scale values.
func Pow10(scale int) (int64, error) {
	if scale < 0 {
		return 0, fmt.Errorf("scale must be >= 0")
	}
	v := int64(1)
	for i := 0; i < scale; i++ {
		if v > (1<<63-1)/10 {
			return 0, fmt.Errorf("scale too large")
		}
		v *= 10
	}
	return v, nil
}
