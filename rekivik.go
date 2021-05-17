package rekivik

import (
	"context"
	"fmt"

	_ "github.com/go-kivik/couchdb/v4" // The CouchDB driver
	"github.com/go-kivik/kivik/v4"
)

type Config struct {
	Protocol string
	Host     string
	Username string
	Password string
}

type Query struct {
	Selector map[string]interface{} `json:"selector"`
}

type ReKivik struct {
	*kivik.Client
}

func NewCouchDB(config Config) (*ReKivik, error) {
	client, err := kivik.New("couch", fmt.Sprintf(
		"%s://%s:%s@%s",
		config.Protocol,
		config.Username,
		config.Password,
		config.Host,
	))

	if err != nil {
		return nil, err
	}

	return &ReKivik{
		Client: client,
	}, nil
}

func (rk *ReKivik) AllDocs(dbName string, dest interface{}) error {
	db := rk.Client.DB(dbName)

	ctx := context.TODO()

	rows, err := db.AllDocs(ctx, kivik.Options{"include_docs": true})
	if err != nil {
		return err
	}

	defer db.Close(ctx)

	for rows.Next() {
		if err = rows.ScanDoc(&dest); err != nil {
			return err
		}
	}
	return nil
}

func (rk *ReKivik) Find(dbName string, dest interface{}, findBy Query) error {
	db := rk.Client.DB(dbName)

	ctx := context.TODO()

	rows, err := db.Find(ctx, findBy, kivik.Options{"include_docs": true})
	if err != nil {
		return err
	}

	defer db.Close(ctx)

	for rows.Next() {
		if err = rows.ScanDoc(&dest); err != nil {
			return err
		}
	}
	return nil
}

func (rk *ReKivik) CreateDoc(dbName string, doc interface{}) (docID string, rev string, err error) {
	db := rk.Client.DB(dbName)

	ctx := context.TODO()

	docID, rev, err = db.CreateDoc(ctx, doc, kivik.Options{"include_docs": true})
	if err != nil {
		return
	}

	defer db.Close(ctx)

	return docID, rev, nil
}

func (rk *ReKivik) Put(dbName string, docID string, doc interface{}) (rev string, err error) {
	db := rk.Client.DB(dbName)

	ctx := context.TODO()

	rev, err = db.Put(ctx, docID, doc, kivik.Options{"include_docs": true})
	if err != nil {
		return "", err
	}

	defer db.Close(ctx)

	return rev, nil
}

func (rk *ReKivik) Delete(dbName string, docID string, rev string) (newRev string, err error) {
	db := rk.Client.DB(dbName)

	ctx := context.TODO()

	newRev, err = db.Delete(ctx, docID, rev, kivik.Options{"include_docs": true})
	if err != nil {
		return "", err
	}

	defer db.Close(ctx)

	return rev, nil
}
