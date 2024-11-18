// Created by lsne on 2022-11-06 21:53:18

package redisutils

import (
	"math"
	"strconv"
	"strings"
)

func Round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func parseSlave(s string) (slaveinfo SlaveInfo, err error) {
	for _, slave := range strings.Split(s, ",") {
		port := ""
		if strings.Contains(slave, "=") {
			kv := strings.Split(slave, "=")
			key := strings.Trim(kv[0], " ")
			value := strings.Trim(kv[1], " ")
			switch key {
			case "ip":
				slaveinfo.IP = value
			case "port":
				port = value
				if slaveinfo.Port, err = strconv.Atoi(value); err != nil {
					return slaveinfo, err
				}
			case "state":
				slaveinfo.State = value
			case "offset":
				if slaveinfo.Offset, err = strconv.ParseUint(value, 10, 64); err != nil {
					return slaveinfo, err
				}
			case "lag":
				if slaveinfo.Lag, err = strconv.ParseUint(value, 10, 64); err != nil {
					return slaveinfo, err
				}
			}
		}
		slaveinfo.IPPort = slaveinfo.IP + ":" + port
	}
	return slaveinfo, nil
}

// func parseDatabase(key, s string) (dbinfo DatabaseInfo, err error) {
// 	dbinfo.DBName = key
// 	for _, db := range strings.Split(s, ",") {
// 		if strings.Contains(db, "=") {
// 			kv := strings.Split(db, "=")
// 			key := strings.Trim(kv[0], " ")
// 			value := strings.Trim(kv[1], " ")
// 			switch key {
// 			case "keys":
// 				if dbinfo.Keys, err = strconv.ParseUint(value, 10, 64); err != nil {
// 					return dbinfo, err
// 				}
// 			case "expires":
// 				if dbinfo.Expires, err = strconv.ParseUint(value, 10, 64); err != nil {
// 					return dbinfo, err
// 				}
// 			case "avg_ttl":
// 				if dbinfo.AvgTtl, err = strconv.ParseUint(value, 10, 64); err != nil {
// 					return dbinfo, err
// 				}
// 			}
// 		}
// 	}
// 	return dbinfo, nil
// }
