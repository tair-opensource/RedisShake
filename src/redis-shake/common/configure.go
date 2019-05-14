package utils

import (
	"strings"
	"fmt"

	"redis-shake/configure"
)

const (
	AddressSplitter       = "@"
	AddressHeaderSplitter = ":"
	AddressClusterSplitter = ";"
)

// parse source address and target address
func ParseAddress(tp string) error {
	// check source
	if tp == conf.TypeDump || tp == conf.TypeSync || tp == conf.TypeRump {
		if err := parseAddress(tp, conf.Options.SourceAddress, conf.Options.SourceType, true); err != nil {
			return err
		}

		if len(conf.Options.SourceAddressList) == 0 {
			return fmt.Errorf("source address shouldn't be empty when type in {dump, sync, rump}")
		}
	}

	// check target
	if tp == conf.TypeRestore || tp == conf.TypeSync || tp == conf.TypeRump {
		if err := parseAddress(tp, conf.Options.TargetAddress, conf.Options.TargetType, false); err != nil {
			return err
		}

		if len(conf.Options.TargetAddressList) == 0 {
			return fmt.Errorf("target address shouldn't be empty when type in {restore, sync, rump}")
		}
	}

	return nil
}

func parseAddress(tp, address, redisType string, isSource bool) error {
	addressLen := len(splitCluster(redisType))
	if addressLen == 0 {
		return fmt.Errorf("address length[%v] illegal", addressLen)
	}

	switch redisType {
	case "":
		fallthrough
	case conf.RedisTypeStandalone:
		if addressLen != 1 {
			return fmt.Errorf("address[%v] length[%v] must == 1 when type is 'standalone'", address, addressLen)
		}
		setAddressList(isSource, address)
	case conf.RedisTypeSentinel:
		arr := strings.Split(address, AddressSplitter)
		if len(arr) != 2 {
			return fmt.Errorf("address[%v] length[%v] != 2", address, len(arr))
		}

		var masterName string
		var fromMaster bool
		if strings.Contains(arr[0], AddressHeaderSplitter) {
			arrHeader := strings.Split(arr[0], AddressHeaderSplitter)
			if isSource {
				masterName = arrHeader[0]
				fromMaster = arrHeader[1] == conf.StandAloneRoleMaster
			} else {
				masterName = arrHeader[0]
				fromMaster = true
			}
		} else {
			masterName = arr[0]
			fromMaster = true
		}

		clusterList := strings.Split(arr[1], AddressClusterSplitter)

		if isSource {
			// get real source
			if source, err := GetReadableRedisAddressThroughSentinel(clusterList, masterName, fromMaster); err != nil {
				return err
			} else {
				conf.Options.SourceAddressList = []string{source}
			}
		} else {
			// get real target
			if target, err := GetWritableRedisAddressThroughSentinel(clusterList, masterName); err != nil {
				return err
			} else {
				conf.Options.TargetAddressList = []string{target}
			}
		}
	case conf.RedisTypeCluster:
		if isSource == false {
			return fmt.Errorf("target type can't be cluster currently")
		}
		setAddressList(isSource, address)
	case conf.RedisTypeProxy:
		if addressLen != 1 {
			return fmt.Errorf("address[%v] length[%v] must == 1 when type is 'proxy'", addressLen, addressLen)
		}
		if isSource && tp != conf.TypeRump {
			return fmt.Errorf("source.type == proxy should only happens when mode is 'rump'")
		}

		setAddressList(isSource, address)
	default:
		return fmt.Errorf("unknown type[%v]", redisType)
	}

	return nil
}

func splitCluster(input string) []string {
	return strings.Split(input, AddressClusterSplitter)
}

func setAddressList(isSource bool, address string) {
	if isSource {
		conf.Options.SourceAddressList = strings.Split(address, AddressClusterSplitter)
	} else {
		conf.Options.TargetAddressList = strings.Split(address, AddressClusterSplitter)
	}
}