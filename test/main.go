package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.etcd.io/etcd/client/v3"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func main() {

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: time.Second * 5,
	})
	defer client.Close()
	if err != nil {
		fmt.Print("connect etcd failed")
	}
	fmt.Print("connect etcd succeed")

	for {
		_, err := client.Put(context.Background(), RandStringBytes(4), RandStringBytes(4))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("put success")
		time.Sleep(time.Second * 1)
	}

}
