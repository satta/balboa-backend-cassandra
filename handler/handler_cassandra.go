package handler

import (
	"fmt"
	"net"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/observation"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

const (
	maxChunk   = 500
	timeFormat = "2006-01-02T15:04:05Z"
)

// CassandraDB is a DB implementation based on Apache Cassandra.
type CassandraHandler struct {
	Cluster      *gocql.ClusterConfig
	Session      *gocql.Session
	StopChan     chan bool
	SendChan     chan []observation.InputObservation
	CurBuffer    []observation.InputObservation
	CurBufferPos int
	Nworkers     int
}

// MakeCassandraHandler returns a new CassandraHandler instance connecting to the
// provided hosts.
func MakeCassandraHandler(hosts []string, username, password string, nofWorkers int) (*CassandraHandler, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "balboa"
	cluster.ProtoVersion = 4
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 5}
	cluster.Consistency = gocql.Quorum
	if len(username) > 0 && len(password) > 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	gsession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	//session := gockle.NewSession(gsession)
	db := &CassandraHandler{
		Cluster:      cluster,
		Session:      gsession,
		StopChan:     make(chan bool),
		SendChan:     make(chan []observation.InputObservation, 100),
		CurBufferPos: 0,
		CurBuffer:    make([]observation.InputObservation, maxChunk),
		Nworkers:     nofWorkers,
	}

	log.Debugf("Firing up %d workers", db.Nworkers)
	for i := 1; i <= db.Nworkers; i++ {
		go db.runChunkWorker()
	}

	return db, nil
}

func (db *CassandraHandler) runChunkWorker() {
	rdataUpd := db.Session.Query(`UPDATE observations_by_rdata SET last_seen = ? where rdata = ? and rrname = ?  and rrtype = ? and sensor_id = ?;`)
	rrnameUpd := db.Session.Query(`UPDATE observations_by_rrname SET last_seen = ? where rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;`)
	firstseenUpd := db.Session.Query(`INSERT INTO observations_firstseen (first_seen, rrname, rdata, rrtype, sensor_id) values (?, ?, ?, ?, ?) IF NOT EXISTS;`)
	countsUpd := db.Session.Query(`UPDATE observations_counts SET count = count + ? where rdata = ? and rrname = ? and rrtype = ? and sensor_id = ?;`)
	for chunk := range db.SendChan {
		select {
		case <-db.StopChan:
			log.Info("database ingest terminated")
			return
		default:
			for _, obs := range chunk {
				if obs.Rdata == "" {
					obs.Rdata = "-"
				}
				if err := rdataUpd.Bind(obs.TimestampEnd, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID).Exec(); err != nil {
					log.Error(err)
					continue
				}
				if err := rrnameUpd.Bind(obs.TimestampEnd, obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID).Exec(); err != nil {
					log.Error(err)
					continue
				}
				if err := firstseenUpd.Bind(obs.TimestampStart, obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID).Exec(); err != nil {
					log.Error(err)
					continue
				}
				if err := countsUpd.Bind(obs.Count, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID).Exec(); err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}
}

// Search returns a slice of observations matching one or more criteria such
// as rdata, rrname, rrtype or sensor ID.
func (db *CassandraHandler) Search(qrdata, qrrname, qrrtype, qsensorID *string) ([]observation.Observation, error) {
	outs := make([]observation.Observation, 0)
	var getQueryString string
	var rdataFirst, hasSecond bool
	var getQuery *gocql.Query

	// determine appropriate table and parameterisation for query
	if qrdata != nil {
		rdataFirst = true
		if qrrname != nil {
			hasSecond = true
			getQueryString = "SELECT * FROM observations_by_rdata WHERE rdata = ? and rrtype = ?"
		} else {
			hasSecond = false
			getQueryString = "SELECT * FROM observations_by_rdata WHERE rdata = ?"
		}
	} else {
		rdataFirst = false
		if qrdata != nil {
			hasSecond = true
			getQueryString = "SELECT * FROM observations_by_rrname WHERE rrname = ? and rdata = ?"
		} else {
			hasSecond = false
			getQueryString = "SELECT * FROM observations_by_rrname WHERE rrname = ?"
		}
	}
	log.Debug(getQueryString)
	getQuery = db.Session.Query(getQueryString)
	getQuery.Consistency(gocql.One)

	// do parameterised search
	if rdataFirst {
		if hasSecond {
			getQuery.Bind(*qrdata, *qrrname)
		} else {
			getQuery.Bind(*qrdata)
		}
	} else {
		if hasSecond {
			getQuery.Bind(*qrrname, *qrdata)
		} else {
			getQuery.Bind(*qrrname)
		}
	}

	// retrieve hits for initial queries
	iter := getQuery.Iter()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}

		if rrnameV, ok := row["rrname"]; ok {
			var rdata, rrname, rrtype, sensorID string
			var lastSeen time.Time
			rrname = rrnameV.(string)
			if rdataV, ok := row["rdata"]; ok {
				rdata = rdataV.(string)

				// secondary filtering by sensor ID and RRType
				if sensorIDV, ok := row["sensor_id"]; ok {
					sensorID = sensorIDV.(string)
					if qsensorID != nil && *qsensorID != sensorID {
						continue
					}
				}
				if rrtypeV, ok := row["rrtype"]; ok {
					rrtype = rrtypeV.(string)
					if qrrtype != nil && *qrrtype != rrtype {
						continue
					}
				}
				if lastSeenV, ok := row["last_seen"]; ok {
					// XXX: check
					lastSeen = lastSeenV.(time.Time)
				}

				// we now have a result item
				out := observation.Observation{
					RRName:   rrname,
					RData:    rdata,
					RRType:   rrtype,
					SensorID: sensorID,
					LastSeen: lastSeen,
				}

				// manual joins -> get additional data from counts table
				tmpMap := make(map[string]interface{})
				getCounters := db.Session.Query(`SELECT count FROM observations_counts WHERE rrname = ? AND rdata = ? AND rrtype = ? AND sensor_id = ?`).Bind(rrname, rdata, rrtype, sensorID)
				err := getCounters.MapScan(tmpMap)
				if err != nil {
					log.Errorf("getCount: %s", err.Error())
					continue
				}
				out.Count = uint(tmpMap["count"].(int64))

				tmpMap = make(map[string]interface{})
				getFirstSeen := db.Session.Query(`SELECT first_seen FROM observations_firstseen WHERE rrname = ? AND rdata = ? AND rrtype = ? AND sensor_id = ?`).Bind(rrname, rdata, rrtype, sensorID)
				err = getFirstSeen.MapScan(tmpMap)
				if err != nil {
					log.Errorf("getFirstSeen: %s", err.Error())
					continue
				}
				// XXX: check
				out.FirstSeen = tmpMap["first_seen"].(time.Time)

				outs = append(outs, out)
			} else {
				log.Warn("result is missing rdata column, something is very wrong")
			}
		} else {
			log.Warn("result is missing rrname column, something is very wrong")
		}

	}

	return outs, nil
}

// Shutdown closes the database connection, leaving the database unable to
// process both reads and writes.
func (db *CassandraHandler) Shutdown() {
	close(db.StopChan)
	if db.Session != nil {
		db.Session.Close()
	}
}

// HandleObservations processes a single observation for insertion.
func (d *CassandraHandler) HandleObservations(obs *observation.InputObservation) {
	if d.CurBufferPos < maxChunk {
		d.CurBuffer[d.CurBufferPos] = *obs
		d.CurBufferPos++
	} else {
		myBuf := d.CurBuffer
		d.CurBuffer = make([]observation.InputObservation, maxChunk)
		d.SendChan <- myBuf
		d.CurBufferPos = 0
		log.Infof("sent chunk of %d", maxChunk)
	}
}

// HandleQuery implements a query for a combination of parameters.
func (d *CassandraHandler) HandleQuery(qr *db.QueryRequest, conn net.Conn) {
	enc := db.MakeEncoder()
	endRes, err := enc.EncodeQueryStreamEndResponse()
	if err != nil {
		log.Error(err)
	}
	conn.Write(endRes.Bytes())
}

// HandleDump is not implemented yet.
func (d *CassandraHandler) HandleDump(dr *db.DumpRequest, conn net.Conn) {
	enc := db.MakeEncoder()
	errRes, err := enc.EncodeErrorResponse(db.ErrorResponse{
		Message: fmt.Sprintf("dump to %s not supported yet", dr.Path),
	})
	if err != nil {
		log.Error(err)
	}
	conn.Write(errRes.Bytes())
}

// HandleBackup is not implemented yet.
func (d *CassandraHandler) HandleBackup(br *db.BackupRequest) {
	log.Error("backup not supported yet")
}
