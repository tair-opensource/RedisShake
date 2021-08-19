package utils

import "time"

func StartQoS(limit int) chan struct{} {
	bucket := make(chan struct{}, limit)
	go func() {
		for range time.NewTicker(1 * time.Second).C {
			for i := 0; i < limit; i++ {
				select {
				case bucket <- struct{}{}:
				default:
					// break if bucket if full
					break
				}

			}
		}
	}()

	return bucket
}
