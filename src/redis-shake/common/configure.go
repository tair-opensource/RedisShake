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
	addressLen := len(splitCluster(address))
	if addressLen == 0 {
		return fmt.Errorf("address length[%v] illegal", addressLen)
	}

	switch redisType {
	case "":
		fallthrough
	case conf.RedisTypeStandalone:
		/*if addressLen != 1 {
			return fmt.Errorf("redis type[%v] address[%v] length[%v] != 1", redisType, address, addressLen)
		}*/
		setAddressList(isSource, address)
	case conf.RedisTypeSentinel:
		arr := strings.Split(address, AddressSplitter)
		if len(arr) != 2 {
			return fmt.Errorf("redis type[%v] address[%v] must begin with or has '%v': e.g., \"master@ip1:port1;ip2:port2\", " +
				"\"@ip1:port1,ip2:port2\"",
				conf.RedisTypeSentinel, address, AddressSplitter)
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
		// get auth type and password
		var auth, password string
		if isSource {
			auth, password = conf.Options.SourceAuthType, conf.Options.SourcePasswordRaw
		} else {
			auth, password = conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw
		}
		tls := isSource && conf.Options.SourceTLSEnable || !isSource && conf.Options.TargetTLSEnable

		if strings.Contains(address, AddressSplitter) {
			arr := strings.Split(address, AddressSplitter)
			if len(arr) != 2 {
				return fmt.Errorf("redis type[%v] address[%v] length[%v] != 2", redisType, address, len(arr))
			}

			if tp == conf.TypeRump && arr[0] == conf.StandAloneRoleSlave {
				return fmt.Errorf("redis role should be master when type is [rump]")
			}
			if isSource && arr[0] != conf.StandAloneRoleSlave && arr[0] != conf.StandAloneRoleMaster {
				return fmt.Errorf("source redis role must be master or slave, when enable automatic discovery with '@'")
			}
			if !isSource && arr[0] != conf.StandAloneRoleMaster && arr[0] != "" {
				return fmt.Errorf("target redis role must be master, when enable automatic discovery with '@'")
			}

			clusterList := strings.Split(arr[1], AddressClusterSplitter)

			role := arr[0]
			if role == "" {
				role = conf.StandAloneRoleMaster
			}

			// create client to fetch
			client := OpenRedisConn(clusterList, auth, password, false, tls)
			if addressList, err := GetAllClusterNode(client, role, "address"); err != nil {
				return err
			} else {
				if isSource {
					conf.Options.SourceAddressList = addressList
				} else {
					conf.Options.TargetAddressList = addressList
				}
			}
		} else {
			// check source list legality: all master or all slave
			addressList := strings.Split(address, AddressClusterSplitter)
			client := OpenRedisConn(addressList, auth, password, false, tls)

			// fetch master address and slave address, ignore error
			masterAddressList, _ := GetAllClusterNode(client, conf.StandAloneRoleMaster, "address")
			slaveAddressList, _ := GetAllClusterNode(client, conf.StandAloneRoleSlave, "address")
			if masterAddressList != nil && slaveAddressList != nil {
				if !CompareUnorderedList(masterAddressList, addressList) && !CompareUnorderedList(slaveAddressList, addressList) {
					endpoint := "source"
					if !isSource {
						endpoint = "target"
					}
					return fmt.Errorf("[%s] redis address should be all masters or all slaves, master:[%v], slave[%v]",
					endpoint, masterAddressList, slaveAddressList)
				}
			}

			// set address
			setAddressList(isSource, address)
		}

	case conf.RedisTypeProxy:
		if isSource && addressLen != 1 {
			return fmt.Errorf("address[%v] length[%v] must == 1 when type is 'proxy'", address, addressLen)
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
