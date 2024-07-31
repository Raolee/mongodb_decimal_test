package main

import (
	"context"
	"fmt"
	"github.com/orlangure/gnomock"
	mongoPreset "github.com/orlangure/gnomock/preset/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sort"
	"sync"
	"time"
)

func main() {
	// gnomock으로 MongoDB 인스턴스 시작
	p := mongoPreset.Preset()
	container, err := gnomock.Start(p)
	if err != nil {
		log.Fatalf("could not start gnomock: %v", err)
	}
	defer gnomock.Stop(container)

	// MongoDB 클라이언트 연결 설정
	uri := fmt.Sprintf("mongodb://%s", container.DefaultAddress())
	clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(64)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	// 데이터베이스 및 컬렉션 설정
	collection := client.Database("decimal_test").Collection("decimal_test_collection")

	// Decimal128 값 생성
	decimal128Val1, err := primitive.ParseDecimal128("100000000000000000000000000000")
	if err != nil {
		log.Fatal(err)
	}

	// 초기 데이터 삽입
	objId := primitive.NewObjectID()
	initialDoc := bson.M{"_id": objId, "value": decimal128Val1}
	_, err = collection.InsertOne(context.TODO(), initialDoc)
	if err != nil {
		log.Fatal(err)
	}

	ScenarioIncDecimal128(collection, objId, 1, 10000)
	ScenarioIncDecimal128(collection, objId, 2, 10000)
	ScenarioIncDecimal128(collection, objId, 4, 10000)
	ScenarioIncDecimal128(collection, objId, 8, 10000)
	ScenarioIncDecimal128(collection, objId, 16, 10000)
	ScenarioIncDecimal128(collection, objId, 32, 10000) // 10000 / 32 = 312.5 이고, 내부적으로 int 취급해서 9984번 연산

	// 업데이트된 문서 조회 및 출력
	filter := bson.M{"_id": objId}
	var updatedDoc bson.M
	err = collection.FindOne(context.TODO(), filter).Decode(&updatedDoc)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated Document: %v\n", updatedDoc)
}

func ScenarioIncDecimal128(collection *mongo.Collection, objId primitive.ObjectID, goroutineCount, opsCount int) {
	// Update 하면서 연산
	totalStartTime := time.Now()
	wg := sync.WaitGroup{}

	durations := make([][]time.Duration, goroutineCount)

	filter := bson.M{"_id": objId, "value": bson.M{"$gt": primitive.NewDecimal128(0, 0)}}
	decValue, _ := primitive.ParseDecimal128("-1")

	for i := 0; i < goroutineCount; i++ {
		durations[i] = make([]time.Duration, opsCount/goroutineCount)
		wg.Add(1)
		go func(num, ops int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				startTime := time.Now()

				update := bson.M{"$inc": bson.M{"value": decValue}}
				_, err := collection.UpdateOne(context.TODO(), filter, update)
				if err != nil {
					log.Fatal(err)
				}

				duration := time.Since(startTime)
				durations[num][j] = duration
			}
		}(i, opsCount/goroutineCount)
	}
	wg.Wait()

	flattenedDurations := make([]time.Duration, 0, opsCount)
	for _, d := range durations {
		flattenedDurations = append(flattenedDurations, d...)
	}

	totalDuration := time.Since(totalStartTime)
	min, max, avg, p50, p95, p99, p999 := recordStats(flattenedDurations)

	fmt.Printf("[Total-%d] Update inc decimal128 %d operations in %v (RPS: %f)\n", goroutineCount, opsCount, totalDuration, float64(opsCount)/totalDuration.Seconds())
	fmt.Printf("\tMin: %v Max: %v Avg: %v\n", min, max, avg)
	fmt.Printf("\tP50: %v P95: %v P99: %v P99.9: %v\n", p50, p95, p99, p999)
}

func recordStats(durations []time.Duration) (min, max, avg, p50, p95, p99, p999 time.Duration) {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	count := len(durations)
	min = durations[0]
	max = durations[count-1]
	avg = time.Duration(int64(totalDuration(durations)) / int64(count))
	p50 = durations[count*50/100]
	p95 = durations[count*95/100]
	p99 = durations[count*99/100]
	p999 = durations[count*999/1000]
	return
}

func totalDuration(durations []time.Duration) time.Duration {
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}
	return total
}
