package retry

import "time"

// Do tries something <retrycount> times, with <retrybackoff * num retries> between each attempt
//  on error. if all retries fail, then it returns the last error
func Do(fn func() error, retrycount int, retrybackoff time.Duration) error {
	var er error
	for i := 1; i <= retrycount; i++ {
		er = fn()
		if er == nil {
			return nil
		}
		time.Sleep(retrybackoff * time.Duration(i))
	}
	return er
}
