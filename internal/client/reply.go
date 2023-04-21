package client

import "RedisShake/internal/log"

func ArrayString(replyInterface interface{}, err error) []string {
	if err != nil {
		log.Panicf(err.Error())
	}
	replyArray := replyInterface.([]interface{})
	replyArrayString := make([]string, len(replyArray))
	for inx, item := range replyArray {
		replyArrayString[inx] = item.(string)
	}
	return replyArrayString
}

func String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return reply.(string), err
}

func Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch v := reply.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		log.Panicf("reply type is not int64 or int, type=%T", v)
	}
	return 0, err
}
