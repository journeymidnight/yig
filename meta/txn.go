package meta

import . "database/sql/driver"

func (m *Meta) NewTrans() (tx Tx, err error) {
	return m.Client.NewTrans()
}

func (m *Meta) CommitTrans(tx Tx) (err error) {
	return m.Client.CommitTrans(tx)
}

func (m *Meta) AbortTrans(tx Tx) (err error) {
	return m.Client.AbortTrans(tx)
}
