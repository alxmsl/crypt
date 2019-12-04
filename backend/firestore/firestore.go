package firestore

import (
	"context"
	"errors"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/bketelsen/crypt/backend"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

type Client struct {
	client     *firestore.Client
	collection string
}

type value struct {
	Data []byte `firestore:"value"`
}

func New(machines []string) (*Client, error) {
	if len(machines) == 0 {
		return nil, errors.New("project should be defined")
	}
	proj, col := splitEndpoint(machines[0])

	opts := []option.ClientOption{}
	opts = append(opts, option.WithGRPCDialOption(grpc.WithBlock()))
	c, err := firestore.NewClient(context.TODO(), proj, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{c, col}, nil
}

func (c *Client) Get(k string) ([]byte, error) {
	return c.GetWithContext(context.TODO(), k)
}

func (c *Client) GetWithContext(ctx context.Context, k string) ([]byte, error) {
	snap, err := c.client.Collection(c.collection).Doc(k).Get(ctx)
	if err != nil {
		return nil, err
	}

	d := &value{}
	err = snap.DataTo(&d)
	if err != nil {
		return nil, err
	}
	return d.Data, nil
}

func (c *Client) List(k string) (backend.KVPairs, error) {
	return c.ListWithContext(context.TODO(), k)
}

func (c *Client) ListWithContext(ctx context.Context, k string) (backend.KVPairs, error) {
	snap, err := c.client.Collection(c.collection).Doc(k).Get(ctx)
	if err != nil {
		return nil, err
	}

	vv := snap.Data()
	pp := make(backend.KVPairs, 0, len(vv))
	for k, v := range vv {
		pp = append(pp, &backend.KVPair{
			Key:   k,
			Value: v.([]byte),
		})
	}
	return pp, nil
}

func (c *Client) Set(k string, v []byte) error {
	return c.SetWithContext(context.TODO(), k, v)
}

func (c *Client) SetWithContext(ctx context.Context, k string, v []byte) error {
	_, err := c.client.Collection(c.collection).Doc(k).Set(ctx, &value{v})
	return err
}

func (c *Client) Watch(k string, stop chan bool) <-chan *backend.Response {
	return c.WatchWithContext(context.TODO(), k, stop)
}

func (c *Client) WatchWithContext(ctx context.Context, k string, stop chan bool) <-chan *backend.Response {
	ch := make(chan *backend.Response, 0)
	t := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-t.C:
				v, err := c.Get(k)
				ch <- &backend.Response{v, err}
				if err != nil {
					time.Sleep(time.Second * 5)
				}
			case <-stop:
				close(ch)
				return
			}
		}
	}()
	return ch
}

func splitEndpoint(e string) (string, string) {
	idx := strings.Index(e, "/")
	if idx < 0 {
		return e, ""
	}
	return e[:idx], strings.Trim(e[idx:], "/")
}
