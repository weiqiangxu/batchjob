package batchjob

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestWithContext(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second*7)
	tests := []struct {
		name  string
		args  args
		want  *ConsumerClient
		want1 context.Context
	}{
		{
			name: "test",
			args: args{
				ctx: ctxWithTimeout,
			},
			want:  nil,
			want1: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var consumer Consumer
			consumer = NewConsumer(tt.args.ctx)
			consumer.Limit(2)
			consumer.AddTask(func() error {
				log.Println("hello john")
				return nil
			})
			consumer.AddTask(func() error {
				log.Println("hello jack")
				return nil
			})
			consumer.AddTask(func() error {
				log.Println("hello tom")
				return nil
			})
			consumer.AddTask(func() error {
				for i := 0; i < 10; i++ {
					time.Sleep(time.Second * time.Duration(i))
					log.Printf("i = %d", i)
				}
				return nil
			})
			t.Log("add task complete")
			err := consumer.Run()
			t.Log("run complete")
			if len(err) != 0 {
				for _, e := range err {
					t.Errorf("err is %#v", e.Error())
				}
			}
		})
	}
}
