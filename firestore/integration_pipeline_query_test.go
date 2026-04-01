// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package firestore

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIntegration_PipelineQuery(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	client := integrationClient(t)
	h := testHelper{t}

	bookDocs := map[string]map[string]any{
		"book1": {
			"title":     "The Hitchhiker's Guide to the Galaxy",
			"author":    "Douglas Adams",
			"genre":     "Science Fiction",
			"published": 1979,
			"rating":    4.2,
			"tags":      []string{"comedy", "space", "adventure"},
			"awards":    map[string]any{"hugo": true, "nebula": false},
			"embedding": Vector64{10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book2": {
			"title":     "Pride and Prejudice",
			"author":    "Jane Austen",
			"genre":     "Romance",
			"published": 1813,
			"rating":    4.5,
			"tags":      []string{"classic", "social commentary", "love"},
			"awards":    map[string]any{"none": true},
			"embedding": Vector64{1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book3": {
			"title":     "One Hundred Years of Solitude",
			"author":    "Gabriel García Márquez",
			"genre":     "Magical Realism",
			"published": 1967,
			"rating":    4.3,
			"tags":      []string{"family", "history", "fantasy"},
			"awards":    map[string]any{"nobel": true, "nebula": false},
			"embedding": Vector64{1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book4": {
			"title":     "The Lord of the Rings",
			"author":    "J.R.R. Tolkien",
			"genre":     "Fantasy",
			"published": 1954,
			"rating":    4.7,
			"tags":      []string{"adventure", "magic", "epic"},
			"awards":    map[string]any{"hugo": false, "nebula": false},
			"cost":      math.NaN(),
			"embedding": Vector64{1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book5": {
			"title":     "The Handmaid's Tale",
			"author":    "Margaret Atwood",
			"genre":     "Dystopian",
			"published": 1985,
			"rating":    4.1,
			"tags":      []string{"feminism", "totalitarianism", "resistance"},
			"awards":    map[string]any{"arthur c. clarke": true, "booker prize": false},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book6": {
			"title":     "Crime and Punishment",
			"author":    "Fyodor Dostoevsky",
			"genre":     "Psychological Thriller",
			"published": 1866,
			"rating":    4.3,
			"tags":      []string{"philosophy", "crime", "redemption"},
			"awards":    map[string]any{"none": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book7": {
			"title":     "To Kill a Mockingbird",
			"author":    "Harper Lee",
			"genre":     "Southern Gothic",
			"published": 1960,
			"rating":    4.2,
			"tags":      []string{"racism", "injustice", "coming-of-age"},
			"awards":    map[string]any{"pulitzer": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0},
		},
		"book8": {
			"title":     "1984",
			"author":    "George Orwell",
			"genre":     "Dystopian",
			"published": 1949,
			"rating":    4.2,
			"tags":      []string{"surveillance", "totalitarianism", "propaganda"},
			"awards":    map[string]any{"prometheus": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0},
		},
		"book9": {
			"title":     "The Great Gatsby",
			"author":    "F. Scott Fitzgerald",
			"genre":     "Modernist",
			"published": 1925,
			"rating":    4.0,
			"tags":      []string{"wealth", "american dream", "love"},
			"awards":    map[string]any{"none": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0},
		},
		"book10": {
			"title":     "Dune",
			"author":    "Frank Herbert",
			"genre":     "Science Fiction",
			"published": 1965,
			"rating":    4.6,
			"tags":      []string{"politics", "desert", "ecology"},
			"awards":    map[string]any{"hugo": true, "nebula": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0},
		},
		"book11": {
			"title":     "Timestamp Book",
			"author":    "Timestamp Author",
			"timestamp": time.Now().Truncate(time.Second),
		},
	}

	coll := client.Collection(collectionIDs.New())
	var docRefs []*DocumentRef
	for id, data := range bookDocs {
		docRef := coll.Doc(id)
		h.mustCreate(docRef, data)
		docRefs = append(docRefs, docRef)
	}
	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})

	t.Run("testAllDataTypes", func(t *testing.T) {
		refDate := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
		refTimestamp := time.Now().Truncate(time.Microsecond)
		refGeoPoint := &latlng.LatLng{Latitude: 1, Longitude: 2}
		refBytes := []byte{1, 2, 3}
		refVector := Vector64{1.0, 2.0, 3.0}

		refMap := map[string]any{
			"number":    int64(1),
			"string":    "a string",
			"boolean":   true,
			"null":      nil,
			"geoPoint":  refGeoPoint,
			"timestamp": refTimestamp,
			"date":      refDate,
			"bytes":     refBytes,
			"vector":    refVector,
		}

		refArray := []any{
			int64(1),
			"a string",
			true,
			nil,
			refTimestamp,
			refGeoPoint,
			refDate,
			refBytes,
			refVector,
		}

		pipeline := client.Pipeline().
			Collection(coll.ID).
			Limit(1).
			Select(Fields(
				ConstantOf(int64(1)).As("number"),
				ConstantOf("a string").As("string"),
				ConstantOf(true).As("boolean"),
				ConstantOfNull().As("null"),
				ConstantOf(refTimestamp).As("timestamp"),
				ConstantOf(refDate).As("date"),
				ConstantOf(refGeoPoint).As("geoPoint"),
				ConstantOf(refBytes).As("bytes"),
				ConstantOf(refVector).As("vector"),
				Map(refMap).As("map"),
				Array(refArray...).As("array"),
			))

		iter := pipeline.Execute(ctx).Results()
		defer iter.Stop()
		res, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}

		data := res.Data()
		if diff := testutil.Diff(data["number"], int64(1)); diff != "" {
			t.Errorf("number mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["string"], "a string"); diff != "" {
			t.Errorf("string mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["boolean"], true); diff != "" {
			t.Errorf("boolean mismatch: %s", diff)
		}
		if data["null"] != nil {
			t.Errorf("got %v, want nil", data["null"])
		}
		if diff := testutil.Diff(data["geoPoint"], refGeoPoint); diff != "" {
			t.Errorf("geoPoint mismatch: %s", diff)
		}
		if !data["timestamp"].(time.Time).Equal(refTimestamp) {
			t.Errorf("got %v, want %v", data["timestamp"], refTimestamp)
		}
		if !data["date"].(time.Time).Equal(refDate) {
			t.Errorf("got %v, want %v", data["date"], refDate)
		}
		if diff := testutil.Diff(data["bytes"], refBytes); diff != "" {
			t.Errorf("bytes mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["vector"], refVector); diff != "" {
			t.Errorf("vector mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["map"], refMap); diff != "" {
			t.Errorf("map mismatch: %s", diff)
		}
	})

	t.Run("testResultMetadata", func(t *testing.T) {
		p := client.Pipeline().Collection(coll.ID)
		snap := p.Execute(ctx)
		iter := snap.Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		execTimePtr, err := snap.ExecutionTime()
		if err != nil {
			t.Fatal(err)
		}
		execTime := *execTimePtr
		if execTime.IsZero() {
			t.Fatal("ExecutionTime is zero")
		}
		for _, res := range results {
			if res.CreateTime().After(*res.UpdateTime()) {
				t.Errorf("CreateTime %v is after UpdateTime %v", res.CreateTime(), res.UpdateTime())
			}
			if res.UpdateTime().After(execTime) {
				t.Errorf("UpdateTime %v is after ExecutionTime %v", res.UpdateTime(), execTime)
			}
		}

		// Update book1 and check again
		_, err = coll.Doc("book1").Update(ctx, []Update{{Path: "rating", Value: 5.0}})
		if err != nil {
			t.Fatal(err)
		}
		// Reset rating back after test
		defer coll.Doc("book1").Update(ctx, []Update{{Path: "rating", Value: 4.2}})

		iter2 := p.Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).Execute(ctx).Results()
		defer iter2.Stop()
		res, err := iter2.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !res.CreateTime().Before(*res.UpdateTime()) {
			t.Errorf("Expected CreateTime %v to be before UpdateTime %v after update", res.CreateTime(), res.UpdateTime())
		}
	})

	t.Run("testResultIsEqual", func(t *testing.T) {
		p := client.Pipeline().Collection(coll.ID).Sort(Orders(FieldOf("title").Ascending()))
		results1, err := p.Limit(1).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		results2, err := p.Limit(1).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		results3, err := p.Offset(1).Limit(1).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(results1) != 1 || len(results2) != 1 || len(results3) != 1 {
			t.Fatalf("Expected 1 result in each snapshot. got %d, %d, %d", len(results1), len(results2), len(results3))
		}

		if results1[0].Ref().Path != results2[0].Ref().Path || !reflect.DeepEqual(results1[0].Data(), results2[0].Data()) {
			t.Error("res1 should be equal to res2")
		}
		if results1[0].Ref().Path == results3[0].Ref().Path {
			t.Error("res1 should not be equal to res3")
		}
	})

	t.Run("testEmptyResultMetadata", func(t *testing.T) {
		snap := client.Pipeline().Collection(coll.ID).Limit(0).Execute(ctx)
		iter := snap.Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 0 {
			t.Errorf("got %d results, want 0", len(results))
		}
		execTime, err := snap.ExecutionTime()
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(*execTime) > 5*time.Second {
			t.Errorf("ExecutionTime %v is too old", execTime)
		}
	})

	t.Run("testAggregateResultMetadata", func(t *testing.T) {
		snap := client.Pipeline().Collection(coll.ID).Aggregate(Accumulators(CountAll().As("count"))).Execute(ctx)
		results, err := snap.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if wantLen := 1; len(results) != wantLen {
			t.Errorf("len(results) got %v want %v", len(results), wantLen)
		}
		if results[0].CreateTime() != nil {
			t.Errorf("Aggregate result should have zero CreateTime, got %v", results[0].CreateTime())
		}
		if results[0].UpdateTime() != nil {
			t.Errorf("Aggregate result should have zero UpdateTime, got %v", results[0].UpdateTime())
		}
		executionTime, err := snap.ExecutionTime()
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(*executionTime) > 3*time.Second {
			t.Errorf("execution time is not recent, not within a tolerance")
		}
	})

	t.Run("testAggregates", func(t *testing.T) {
		res1, err := client.Pipeline().Collection(coll.ID).Aggregate(Accumulators(CountAll().As("count"))).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, res1, []map[string]any{{"count": int64(11)}})

		res2, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("genre", "Science Fiction")).
			Aggregate(Accumulators(
				CountAll().As("count"),
				Average("rating").As("avg_rating"),
				FieldOf("rating").Maximum().As("max_rating"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, res2, []map[string]any{{"count": int64(2), "avg_rating": 4.4, "max_rating": 4.6}})
	})

	t.Run("testMoreAggregates", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Aggregate(Accumulators(
			Sum("rating").As("sum_rating"),
			Count("rating").As("count_rating"),
			CountDistinct("genre").As("distinct_genres"),
		)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if math.Abs(data["sum_rating"].(float64)-43.1) > 0.00001 {
			t.Errorf("got sum_rating %v, want 43.1", data["sum_rating"])
		}
		if data["count_rating"] != int64(10) {
			t.Errorf("got count_rating %v, want 10", data["count_rating"])
		}
		if data["distinct_genres"] != int64(8) {
			t.Errorf("got distinct_genres %v, want 8", data["distinct_genres"])
		}
	})

	t.Run("testCountIfAggregate", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Aggregate(Accumulators(
			CountIf(GreaterThan(FieldOf("rating"), 4.3)).As("count"),
		)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, res, []map[string]any{{"count": int64(3)}})
	})

	t.Run("testGroupBysWithoutAccumulators", func(t *testing.T) {
		iter := client.Pipeline().CreateFromQuery(coll).Where(LessThan("published", 1900)).
			Aggregate(nil, WithAggregateGroups("genre"))
		_, err := iter.Execute(ctx).Results().Next()
		if err == nil || !strings.Contains(err.Error(), "requires at least one accumulator") {
			t.Errorf("Expected error containing 'requires at least one accumulator', got: %v", err)
		}
	})

	t.Run("testDistinct", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1900)).
			Distinct(Fields(FieldOf("genre").ToLower().As("lower_genre"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData := []map[string]any{
			{"lower_genre": "romance"},
			{"lower_genre": "psychological thriller"},
		}
		containsExactly(t, results, wantData)
	})

	t.Run("testGroupBysAndAggregate", func(t *testing.T) {
		iter := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1984)).
			Aggregate(
				Accumulators(Average("rating").As("avg_rating")),
				WithAggregateGroups("genre"),
			).
			Where(GreaterThan("avg_rating", 4.3))
		results, err := iter.Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData := []map[string]any{
			{"avg_rating": 4.7, "genre": "Fantasy"},
			{"avg_rating": 4.5, "genre": "Romance"},
			{"avg_rating": 4.4, "genre": "Science Fiction"},
		}

		containsExactly(t, results, wantData)
	})

	t.Run("testMinMax", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Aggregate(Accumulators(
			CountAll().As("count"),
			FieldOf("rating").Maximum().As("max_rating"),
			FieldOf("published").Minimum().As("min_published"),
		)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{
			"count":         int64(11),
			"max_rating":    4.7,
			"min_published": int64(1813),
		}})
	})

	t.Run("testFirstAndLastAccumulators", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("published").GreaterThan(0)).
			Sort(Orders(FieldOf("published").Ascending())).
			Aggregate(Accumulators(
				First("rating").As("firstBookRating"),
				First("title").As("firstBookTitle"),
				Last("rating").As("lastBookRating"),
				Last("title").As("lastBookTitle"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["firstBookRating"] != 4.5 || data["firstBookTitle"] != "Pride and Prejudice" {
			t.Errorf("First mismatch: %v", data)
		}
		if data["lastBookRating"] != 4.1 || data["lastBookTitle"] != "The Handmaid's Tale" {
			t.Errorf("Last mismatch: %v", data)
		}
	})

	t.Run("testFirstAndLastAccumulatorsWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("published").GreaterThan(0)).
			Sort(Orders(FieldOf("published").Ascending())).
			Aggregate(Accumulators(
				FieldOf("rating").First().As("firstBookRating"),
				FieldOf("title").First().As("firstBookTitle"),
				FieldOf("rating").Last().As("lastBookRating"),
				FieldOf("title").Last().As("lastBookTitle"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["firstBookRating"] != 4.5 || data["firstBookTitle"] != "Pride and Prejudice" {
			t.Errorf("First mismatch: %v", data)
		}
		if data["lastBookRating"] != 4.1 || data["lastBookTitle"] != "The Handmaid's Tale" {
			t.Errorf("Last mismatch: %v", data)
		}
	})

	t.Run("testArrayAggAccumulators", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("published").GreaterThan(0)).
			Sort(Orders(FieldOf("published").Ascending())).
			Aggregate(Accumulators(ArrayAgg("rating").As("allRatings"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["allRatings"].([]any)
		want := []any{4.5, 4.3, 4.0, 4.2, 4.7, 4.2, 4.6, 4.3, 4.2, 4.1}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("allRatings mismatch: %s", diff)
		}
	})

	t.Run("testArrayAggDistinctAccumulators", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("published").GreaterThan(0)).
			Aggregate(Accumulators(ArrayAggDistinct("rating").As("allDistinctRatings"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["allDistinctRatings"].([]any)
		sort.Slice(got, func(i, j int) bool { return got[i].(float64) < got[j].(float64) })
		want := []any{4.0, 4.1, 4.2, 4.3, 4.5, 4.6, 4.7}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("allDistinctRatings mismatch: %s", diff)
		}
	})

	t.Run("testArrayAggDistinctAccumulatorsWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("published").GreaterThan(0)).
			Aggregate(Accumulators(FieldOf("rating").ArrayAggDistinct().As("allDistinctRatings"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["allDistinctRatings"].([]any)
		sort.Slice(got, func(i, j int) bool { return got[i].(float64) < got[j].(float64) })
		want := []any{4.0, 4.1, 4.2, 4.3, 4.5, 4.6, 4.7}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("allDistinctRatings mismatch: %s", diff)
		}
	})

	t.Run("selectSpecificFields", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Select(Fields("title", "author")).
			Sort(Orders(FieldOf("author").Ascending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy", "author": "Douglas Adams"},
			{"title": "The Great Gatsby", "author": "F. Scott Fitzgerald"},
			{"title": "Dune", "author": "Frank Herbert"},
			{"title": "Crime and Punishment", "author": "Fyodor Dostoevsky"},
			{"title": "One Hundred Years of Solitude", "author": "Gabriel García Márquez"},
			{"title": "1984", "author": "George Orwell"},
			{"title": "To Kill a Mockingbird", "author": "Harper Lee"},
			{"title": "The Lord of the Rings", "author": "J.R.R. Tolkien"},
			{"title": "Pride and Prejudice", "author": "Jane Austen"},
			{"title": "The Handmaid's Tale", "author": "Margaret Atwood"},
			{"title": "Timestamp Book", "author": "Timestamp Author"},
		})
	})

	t.Run("addAndRemoveFields", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("author").NotEqual("Timestamp Author")).
			AddFields(Selectables(
				StringConcat(FieldOf("author"), "_", FieldOf("title")).As("author_title"),
				StringConcat(FieldOf("title"), "_", FieldOf("author")).As("title_author"),
			)).
			RemoveFields(Fields("title_author", "tags", "awards", "rating", "title", "embedding", "cost")).
			RemoveFields(Fields(FieldOf("published"), FieldOf("genre"), FieldOf("nestedField"))).
			Sort(Orders(FieldOf("author_title").Ascending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{
			{"author_title": "Douglas Adams_The Hitchhiker's Guide to the Galaxy", "author": "Douglas Adams"},
			{"author_title": "F. Scott Fitzgerald_The Great Gatsby", "author": "F. Scott Fitzgerald"},
			{"author_title": "Frank Herbert_Dune", "author": "Frank Herbert"},
			{"author_title": "Fyodor Dostoevsky_Crime and Punishment", "author": "Fyodor Dostoevsky"},
			{"author_title": "Gabriel García Márquez_One Hundred Years of Solitude", "author": "Gabriel García Márquez"},
			{"author_title": "George Orwell_1984", "author": "George Orwell"},
			{"author_title": "Harper Lee_To Kill a Mockingbird", "author": "Harper Lee"},
			{"author_title": "J.R.R. Tolkien_The Lord of the Rings", "author": "J.R.R. Tolkien"},
			{"author_title": "Jane Austen_Pride and Prejudice", "author": "Jane Austen"},
			{"author_title": "Margaret Atwood_The Handmaid's Tale", "author": "Margaret Atwood"},
		})
	})

	t.Run("whereByMultipleConditions", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(And(GreaterThan("rating", 4.5), Equal("genre", "Science Fiction"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		ds, err := coll.Doc("book10").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{ds.Data()})
		if results[0].Ref().Path != ds.Ref.Path {
			t.Errorf("Expected reference path %v, got %v", ds.Ref.Path, results[0].Ref().Path)
		}
	})

	t.Run("whereByOrCondition", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Or(Equal("genre", "Romance"), Equal("genre", "Dystopian"))).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{
			{"title": "Pride and Prejudice"},
			{"title": "The Handmaid's Tale"},
			{"title": "1984"},
		})
	})

	t.Run("testPipelineWithOffsetAndLimit", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Sort(Orders(FieldOf("author").Ascending())).
			Offset(5).
			Limit(3).
			Select(Fields("title", "author")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{
			{"title": "1984", "author": "George Orwell"},
			{"title": "To Kill a Mockingbird", "author": "Harper Lee"},
			{"title": "The Lord of the Rings", "author": "J.R.R. Tolkien"},
		})
	})

	t.Run("testArrayContains", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayContains("tags", "comedy")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		ds, err := coll.Doc("book1").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{ds.Data()})
	})

	t.Run("testArrayContainsAny", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayContainsAny("tags", []string{"comedy", "classic"})).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy"},
			{"title": "Pride and Prejudice"},
		})
	})

	t.Run("testArrayContainsAll", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayContainsAll("tags", []string{"adventure", "magic"})).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, results, []map[string]any{{"title": "The Lord of the Rings"}})
	})

	t.Run("testArrayLength", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("tags").ArrayLength().As("tagsCount"))).
			Where(Equal("tagsCount", 3)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 10 {
			t.Errorf("got %d results, want 10", len(results))
		}
	})

	t.Run("testArrayConcat", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("tags").ArrayConcat(Array("newTag1", "newTag2")).As("modifiedTags"))).
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{"modifiedTags": []any{"comedy", "space", "adventure", "newTag1", "newTag2"}},
		}
		isEqualTo(t, results, want)
	})

	t.Run("testStrConcat", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(StringConcat(FieldOf("author"), " - ", FieldOf("title")).As("bookInfo"))).
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{"bookInfo": "Douglas Adams - The Hitchhiker's Guide to the Galaxy"},
		}
		isEqualTo(t, []*PipelineResult{res}, want)
	})

	t.Run("testStartsWith", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(StartsWith("title", "The")).
			Select(Fields("title")).
			Sort(Orders(FieldOf("title").Ascending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{"title": "The Great Gatsby"},
			{"title": "The Handmaid's Tale"},
			{"title": "The Hitchhiker's Guide to the Galaxy"},
			{"title": "The Lord of the Rings"},
		}
		isEqualTo(t, results, want)
	})

	t.Run("testEndsWith", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(EndsWith(FieldOf("title"), ConstantOf("y"))).
			Select(Fields("title")).
			Sort(Orders(FieldOf("title").Descending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy"},
			{"title": "The Great Gatsby"},
		}
		isEqualTo(t, results, want)
	})

	t.Run("testLength", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("title").CharLength().As("titleLength"), FieldOf("title"))).
			Where(GreaterThan("titleLength", 21)).
			Sort(Orders(FieldOf("titleLength").Descending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{"titleLength": int64(36), "title": "The Hitchhiker's Guide to the Galaxy"},
			{"titleLength": int64(29), "title": "One Hundred Years of Solitude"},
		}
		isEqualTo(t, results, want)
	})

	t.Run("testStringFunctions", func(t *testing.T) {
		// Reverse
		res, err := client.Pipeline().Collection(coll.ID).
			Select(Fields(FieldOf("title").Reverse().As("reversed_title"), FieldOf("author"))).
			Where(FieldOf("author").Equal("Douglas Adams")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := "yxalaG eht ot ediuG s'rekihhctiH ehT"
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"reversed_title": want, "author": "Douglas Adams"}})

		// CharLength
		res2, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("title").CharLength().As("title_length"), FieldOf("author"))).
			Where(FieldOf("author").Equal("Douglas Adams")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res2}, []map[string]any{{"title_length": int64(36), "author": "Douglas Adams"}})

		// ByteLength
		res3, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("author"), FieldOf("title").StringConcat("_银河系漫游指南").ByteLength().As("title_byte_length"))).
			Where(FieldOf("author").Equal("Douglas Adams")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res3}, []map[string]any{{"title_byte_length": int64(58), "author": "Douglas Adams"}})
	})

	t.Run("testToLowercase", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("title").ToLower().As("lowercaseTitle"))).
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{
			"lowercaseTitle": "the hitchhiker's guide to the galaxy",
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{want})
	})

	t.Run("testToUppercase", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("author").ToUpper().As("uppercaseAuthor"))).
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{
			"uppercaseAuthor": "DOUGLAS ADAMS",
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{want})
	})

	t.Run("testTrim", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			AddFields(Selectables(StringConcat(ConstantOf(" "), FieldOf("title"), ConstantOf(" ")).As("spacedTitle"))).
			Select(Fields(FieldOf("spacedTitle").Trim().As("trimmedTitle"), FieldOf("spacedTitle"))).
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{
			"spacedTitle":  " The Hitchhiker's Guide to the Galaxy ",
			"trimmedTitle": "The Hitchhiker's Guide to the Galaxy",
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{want})

	})

	t.Run("testTrimWithCharacters", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			AddFields(Selectables(Concat(ConstantOf("_-"), FieldOf("title"), ConstantOf("-_")).As("paddedTitle"))).
			Select(Fields(FieldOf("paddedTitle").TrimValue("_-").As("trimmedTitle"), FieldOf("paddedTitle"))).
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData := []map[string]any{
			{"paddedTitle": "_-The Hitchhiker's Guide to the Galaxy-_",
				"trimmedTitle": "The Hitchhiker's Guide to the Galaxy"},
		}
		isEqualTo(t, res, wantData)
	})

	t.Run("testLike", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("title").Like("%Guide%")).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData := []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy"},
		}
		isEqualTo(t, results, wantData)
	})

	t.Run("testRegexContains", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("title").RegexContains("(?i)(the|of)")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 5 {
			t.Errorf("got %d results, want 5", len(results))
		}
	})

	t.Run("testRegexFind", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("title").RegexFind("^\\w+").As("firstWordInTitle"))).
			Sort(Orders(FieldOf("firstWordInTitle").Ascending())).
			Limit(3).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		wantData := []map[string]any{
			{"firstWordInTitle": "1984"},
			{"firstWordInTitle": "Crime"},
			{"firstWordInTitle": "Dune"},
		}
		isEqualTo(t, results, wantData)
	})

	t.Run("testRegexFindAll", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(FieldOf("title").RegexFindAll("\\w+").As("wordsInTitle"))).
			Sort(Orders(FieldOf("wordsInTitle").Ascending())).
			Limit(3).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		wantData := []map[string]any{
			{"wordsInTitle": []any{"1984"}},
			{"wordsInTitle": []any{"Crime", "and", "Punishment"}},
			{"wordsInTitle": []any{"Dune"}},
		}
		isEqualTo(t, results, wantData)
	})

	t.Run("testRegexMatches", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(RegexMatch("title", ".*(?i)(the|of).*")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 5 {
			t.Errorf("got %d results, want 5", len(results))
		}
	})

	t.Run("testArithmeticOperations", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Select(Fields(
				Add(FieldOf("rating"), 1).As("ratingPlusOne"),
				Subtract(FieldOf("published"), 1900).As("yearsSince1900"),
				FieldOf("rating").Multiply(10).As("ratingTimesTen"),
				FieldOf("rating").Divide(2).As("ratingDividedByTwo"),
			)).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		wantData := map[string]any{
			"ratingPlusOne":      5.2,
			"yearsSince1900":     int64(79),
			"ratingTimesTen":     42.0,
			"ratingDividedByTwo": 2.1,
		}

		isEqualTo(t, []*PipelineResult{res}, []map[string]any{wantData})
	})

	t.Run("testComparisonOperators", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(And(
				GreaterThan("rating", 4.2),
				FieldOf("rating").LessThanOrEqual(4.5),
				NotEqual("genre", "Science Fiction"),
			)).
			Select(Fields("rating", "title")).
			Sort(Orders(FieldOf("title").Ascending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData := []map[string]any{
			{"rating": 4.3, "title": "Crime and Punishment"},
			{"rating": 4.3, "title": "One Hundred Years of Solitude"},
			{"rating": 4.5, "title": "Pride and Prejudice"},
		}
		isEqualTo(t, results, wantData)
	})

	t.Run("testLogicalAndComparisonOperators", func(t *testing.T) {
		// test XOR
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Xor(
				Equal("genre", "Romance"),
				Equal("genre", "Dystopian"),
				Equal("genre", "Fantasy"),
				Equal("published", 1949),
			)).Select(Fields("title")).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
		wantData := []map[string]any{
			{"title": "Pride and Prejudice"},
			{"title": "The Lord of the Rings"},
			{"title": "The Handmaid's Tale"},
		}
		containsExactly(t, results, wantData)

		// test EqualAny
		results2, err := client.Pipeline().CreateFromQuery(coll).
			Where(EqualAny("genre", []string{"Romance", "Dystopian"})).
			Select(Fields("title")).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData2 := []map[string]any{
			{"title": "Pride and Prejudice"},
			{"title": "The Handmaid's Tale"},
			{"title": "1984"},
		}
		containsExactly(t, results2, wantData2)

		// test NotEqualAny
		results3, err := client.Pipeline().CreateFromQuery(coll).
			Where(NotEqualAny("genre", []any{"Science Fiction", "Romance", "Dystopian", nil})).
			Select(Fields("genre")).Distinct(Fields("genre")).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results3) != 6 {
			t.Errorf("got %d results, want 6", len(results3))
		}
		wantData3 := []map[string]any{
			{"genre": nil},
			{"genre": "Magical Realism"},
			{"genre": "Fantasy"},
			{"genre": "Psychological Thriller"},
			{"genre": "Southern Gothic"},
			{"genre": "Modernist"},
		}
		containsExactly(t, results3, wantData3)
	})

	t.Run("testCondExpression", func(t *testing.T) {
		// Go has Conditional
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("title").NotEqual("Timestamp Book")).
			Select(Fields(
				Conditional(GreaterThan(FieldOf("published"), 1980), "Modern", "Classic").As("era"),
				FieldOf("title"),
				FieldOf("published"),
			)).
			Sort(Orders(FieldOf("published").Ascending())).
			Limit(2).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
		if results[0].Data()["era"] != "Classic" || results[0].Data()["title"] != "Pride and Prejudice" {
			t.Errorf("mismatch: %v", results[0].Data())
		}
		if results[1].Data()["era"] != "Classic" || results[1].Data()["title"] != "Crime and Punishment" {
			t.Errorf("mismatch: %v", results[1].Data())
		}
	})

	t.Run("testLogicalOperators", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Or(
				And(GreaterThan("rating", 4.5), Equal("genre", "Science Fiction")),
				LessThan("published", 1900),
			)).
			Select(Fields("title")).
			Sort(Orders(FieldOf("title").Ascending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantData := []map[string]any{
			{"title": "Crime and Punishment"},
			{"title": "Dune"},
			{"title": "Pride and Prejudice"},
		}

		if len(results) != len(wantData) {
			t.Errorf("got %d results, want %d", len(results), len(wantData))
		}
		for i, res := range results {
			if !reflect.DeepEqual(res.Data(), wantData[i]) {
				t.Errorf("mismatch: got: %v, want: %v", res.Data(), wantData[i])
			}
		}
	})

	t.Run("testChecks", func(t *testing.T) {
		t.Skip("Skipping functional test failure")
		res, err := client.Pipeline().Collection(coll.ID).
			Sort(Orders(FieldOf("rating").Descending())).
			Limit(1).
			Select(Fields(
				FieldOf("rating").Equal(ConstantOfNull()).As("ratingIsNull"),
				FieldOf("rating").Equal(math.NaN()).As("ratingIsNaN"),
				ArrayGet("title", 0).IsError().As("isError"),
				ArrayGet("title", 0).IfError(ConstantOf("was error")).As("ifError"),
				FieldOf("foo").IsAbsent().As("isAbsent"),
				FieldOf("title").NotEqual(ConstantOfNull()).As("titleIsNotNull"),
				FieldOf("cost").NotEqual(math.NaN()).As("costIsNotNan"),
				FieldOf("fooBarBaz").FieldExists().As("fooBarBazExists"),
				FieldOf("title").FieldExists().As("titleExists"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		wantData := map[string]any{
			"ratingIsNull":    false,
			"ratingIsNaN":     false,
			"isError":         false,
			"isAbsent":        true,
			"titleIsNotNull":  true,
			"costIsNotNan":    false,
			"fooBarBazExists": false,
			"titleExists":     true,
		}
		if !reflect.DeepEqual(data, wantData) {
			t.Errorf("checks mismatch: got: %v, want: %v", data, wantData)
		}
	})

	t.Run("testLogicalMinMax", func(t *testing.T) {
		// LogicalMaximum
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("author").Equal("Douglas Adams")).
			Select(Fields(
				FieldOf("rating").LogicalMaximum(4.5).As("max_rating"),
				LogicalMaximum(FieldOf("published"), 1900).As("max_published"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{"max_rating": 4.5, "max_published": int64(1979)}})
		// LogicalMinimum
		res2, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("author").Equal("Douglas Adams")).
			Select(Fields(
				FieldOf("rating").LogicalMinimum(4.5).As("min_rating"),
				LogicalMinimum(FieldOf("published"), 1900).As("min_published"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res2, []map[string]any{{"min_rating": 4.2, "min_published": int64(1900)}})
	})

	t.Run("testMapGet", func(t *testing.T) {
		gotResults, err := client.Pipeline().Collection(coll.ID).
			Select(Fields(FieldOf("awards").MapGet("hugo").As("hugoAward"), FieldOf("title"))).
			Where(Equal("hugoAward", true)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		wantResults := []map[string]any{
			{"hugoAward": true, "title": "The Hitchhiker's Guide to the Galaxy"},
			{"hugoAward": true, "title": "Dune"},
		}
		containsExactly(t, gotResults, wantResults)
	})

	t.Run("testDataManipulationExpressions", func(t *testing.T) {
		// test timestamp manipulation
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Timestamp Book")).
			Select(Fields(
				TimestampAdd(FieldOf("timestamp"), "day", 1).As("timestamp_plus_day"),
				TimestampSubtract(FieldOf("timestamp"), "hour", 1).As("timestamp_minus_hour"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		original := bookDocs["book11"]["timestamp"].(time.Time)
		gotPlusDay := res.Data()["timestamp_plus_day"].(time.Time)
		gotMinusHour := res.Data()["timestamp_minus_hour"].(time.Time)
		if !gotPlusDay.Equal(original.Add(24 * time.Hour)) {
			t.Errorf("timestampAdd mismatch: %v vs %v", gotPlusDay, original.Add(24*time.Hour))
		}
		if !gotMinusHour.Equal(original.Add(-1 * time.Hour)) {
			t.Errorf("timestampSubtract mismatch: %v", gotMinusHour)
		}

		// test array/map manipulation
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(
				ArrayGet("tags", 1).As("second_tag"),
				MapMerge(FieldOf("awards"), Map(map[string]any{"new_award": true})).As("merged_awards"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		wantRes2 := map[string]any{
			"second_tag":    "space",
			"merged_awards": map[string]any{"hugo": true, "nebula": false, "new_award": true},
		}
		if !reflect.DeepEqual(res2.Data(), wantRes2) {
			t.Errorf("second_tag mismatch: got: %v, want: %v", res2.Data(), wantRes2)
		}

		res3, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(
				ArrayReverse("tags").As("reversed_tags"),
				MapRemove(FieldOf("awards"), "nebula").As("removed_awards"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		wantRes3 := map[string]any{
			"reversed_tags":  []any{"adventure", "space", "comedy"},
			"removed_awards": map[string]any{"hugo": true},
		}
		if !reflect.DeepEqual(res3.Data(), wantRes3) {
			t.Errorf("reversed_tags mismatch: got: %v, want: %v", res3.Data(), wantRes3)
		}
	})

	t.Run("testTimestampTrunc", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Timestamp Book")).
			Select(Fields(
				TimestampTruncate(FieldOf("timestamp"), "year").As("trunc_year"),
				TimestampTruncate(FieldOf("timestamp"), "month").As("trunc_month"),
				TimestampTruncate(FieldOf("timestamp"), "day").As("trunc_day"),
				TimestampTruncate(FieldOf("timestamp"), "hour").As("trunc_hour"),
				TimestampTruncate(FieldOf("timestamp"), "minute").As("trunc_minute"),
				TimestampTruncate(FieldOf("timestamp"), "second").As("trunc_second"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		wantTime := bookDocs["book11"]["timestamp"].(time.Time)
		gotData := res.Data()
		if wantDate := time.Date(wantTime.Year(), time.January, 1, 0, 0, 0, 0, time.UTC); gotData["trunc_year"].(time.Time) != wantDate {
			t.Errorf("trunc_year mismatch: got: %v, want: %v", gotData["trunc_year"], wantDate)
		}
		if wantDate := time.Date(wantTime.Year(), wantTime.Month(), 1, 0, 0, 0, 0, time.UTC); gotData["trunc_month"].(time.Time) != wantDate {
			t.Errorf("trunc_month mismatch: got: %v, want: %v", gotData["trunc_month"], wantDate)
		}
		if wantDate := time.Date(wantTime.Year(), wantTime.Month(), wantTime.Day(), 0, 0, 0, 0, time.UTC); gotData["trunc_day"].(time.Time) != wantDate {
			t.Errorf("trunc_day mismatch: got: %v, want: %v", gotData["trunc_day"], wantDate)
		}
		if wantDate := time.Date(wantTime.Year(), wantTime.Month(), wantTime.Day(), wantTime.Hour(), 0, 0, 0, time.UTC); gotData["trunc_hour"].(time.Time) != wantDate {
			t.Errorf("trunc_hour mismatch: got: %v, want: %v", gotData["trunc_hour"], wantDate)
		}
		if wantDate := time.Date(wantTime.Year(), wantTime.Month(), wantTime.Day(), wantTime.Hour(), wantTime.Minute(), 0, 0, time.UTC); gotData["trunc_minute"].(time.Time) != wantDate {
			t.Errorf("trunc_minute mismatch: got: %v, want: %v", gotData["trunc_minute"], wantDate)
		}
		if wantDate := time.Date(wantTime.Year(), wantTime.Month(), wantTime.Day(), wantTime.Hour(), wantTime.Minute(), wantTime.Second(), 0, time.UTC); gotData["trunc_second"].(time.Time) != wantDate {
			t.Errorf("trunc_second mismatch: got: %v, want: %v", gotData["trunc_second"], wantDate)
		}
	})

	t.Run("testMathExpressions", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(
				Ceil(FieldOf("rating")).As("ceil_rating"),
				Floor(FieldOf("rating")).As("floor_rating"),
				Pow(FieldOf("rating"), 2).As("pow_rating"),
				Round(FieldOf("rating")).As("round_rating"),
				Sqrt(FieldOf("rating")).As("sqrt_rating"),
				FieldOf("published").Mod(10).As("mod_published"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["ceil_rating"] != 5.0 {
			t.Errorf("ceil_rating mismatch: %v", data["ceil_rating"])
		}
		if data["floor_rating"] != 4.0 {
			t.Errorf("floor_rating mismatch: %v", data["floor_rating"])
		}
		if math.Abs(data["pow_rating"].(float64)-17.64) > 0.00001 {
			t.Errorf("pow_rating mismatch: %v", data["pow_rating"])
		}
		if data["round_rating"] != 4.0 {
			t.Errorf("round_rating mismatch: %v", data["round_rating"])
		}
		if math.Abs(data["sqrt_rating"].(float64)-2.04939) > 0.00001 {
			t.Errorf("sqrt_rating mismatch: %v", data["sqrt_rating"])
		}
		if data["mod_published"] != int64(9) {
			t.Errorf("mod_published mismatch: %v", data["mod_published"])
		}
	})

	t.Run("testAdvancedMathExpressions", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Fields(
				Exp(FieldOf("rating")).As("exp_rating"),
				Ln(FieldOf("rating")).As("ln_rating"),
				Log(FieldOf("rating"), 10).As("log_rating"),
				FieldOf("rating").Log10().As("log10_rating"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if math.Abs(data["exp_rating"].(float64)-109.94717) > 0.001 {
			t.Errorf("exp_rating mismatch: %v", data["exp_rating"])
		}
		if math.Abs(data["ln_rating"].(float64)-1.54756) > 0.001 {
			t.Errorf("ln_rating mismatch: %v", data["ln_rating"])
		}
		if math.Abs(data["log_rating"].(float64)-0.67209) > 0.001 {
			t.Errorf("log_rating mismatch: %v", data["log_rating"])
		}
		if math.Abs(data["log10_rating"].(float64)-0.67209) > 0.001 {
			t.Errorf("log10_rating mismatch: %v", data["log10_rating"])
		}
	})

	t.Run("testRand", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Select(Fields(Rand().As("randomNumber"))).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		r := res.Data()["randomNumber"].(float64)
		if r < 0.0 || r >= 1.0 {
			t.Errorf("invalid rand: %v", r)
		}
	})

	t.Run("testTrunc", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("title").Equal("Pride and Prejudice")).
			Limit(1).
			Select(Fields(Trunc("rating").As("truncatedRating"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["truncatedRating"] != 4.0 {
			t.Errorf("got %v, want 4.0", res.Data()["truncatedRating"])
		}
	})

	t.Run("testTruncToPrecision", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(Fields(
				TruncToPrecision(ConstantOf(4.123456), 0).As("p0"),
				TruncToPrecision(ConstantOf(4.123456), 1).As("p1"),
				TruncToPrecision(ConstantOf(4.123456), 2).As("p2"),
				TruncToPrecision(ConstantOf(4.123456), 4).As("p4"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["p0"] != 4.0 || data["p1"] != 4.1 || data["p2"] != 4.12 || data["p4"] != 4.1234 {
			t.Errorf("truncToPrecision mismatch: %v", data)
		}
	})

	t.Run("testConcat", func(t *testing.T) {
		// String concat
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Concat(FieldOf("author"), " ", FieldOf("title")).As("author_title"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["author_title"] != "Douglas Adams The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("string concat mismatch got %v want %v", res.Data()["author_title"], "Douglas Adams The Hitchhiker's Guide to the Galaxy")
		}

		// Array concat
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Concat(FieldOf("tags"), []string{"newTag"}).As("new_tags"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(res2.Data()["new_tags"].([]any), []any{"comedy", "space", "adventure", "newTag"}) {
			t.Errorf("array concat mismatch got %v want %v", res2.Data()["new_tags"], []any{"comedy", "space", "adventure", "newTag"})
		}

		// Blob concat
		b1 := []byte{1, 2}
		b2 := []byte{3, 4}
		res3, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(Fields(Concat(ConstantOf(b1), b2).As("concatenated_blob"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(res3.Data()["concatenated_blob"].([]byte), []byte{1, 2, 3, 4}) {
			t.Errorf("blob concat mismatch got %v want %v", res3.Data()["concatenated_blob"], []byte{1, 2, 3, 4})
		}

		// Mismatched types should just fail.
		_, err = client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Concat(FieldOf("title"), FieldOf("tags")).As("mismatched"))).
			Execute(ctx).Results().Next()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected invalid argument error for mismatched types, got %v", err)
		}
	})

	t.Run("testCurrentTimestamp", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(Fields(CurrentTimestamp().As("now"))).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 {
			t.Errorf("expected 1 result, got %d", len(res))
		}
		now, ok := res[0].Data()["now"].(time.Time)
		if !ok {
			t.Errorf("expected time.Time, got %T", res[0].Data()["now"])
		}
		if time.Since(now) > 10*time.Second {
			t.Errorf("current timestamp too far in past: %v", now)
		}
	})

	t.Run("testIfAbsent", func(t *testing.T) {
		// Case 1: Field is present, should return the field value.
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(FieldOf("rating").IfAbsent(0.0).As("rating_or_default"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{"rating_or_default": 4.2}})

		// Case 2: Field is absent, should return the default value.
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(IfAbsent(FieldOf("non_existent_field"), "default").As("field_or_default"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res2, []map[string]any{{"field_or_default": "default"}})

		// Case 3: Field is present and null, should return null.
		_, err = coll.Doc("bookWithNull").Set(ctx, map[string]any{"title": "Book With Null", "optional_field": nil})
		if err != nil {
			t.Fatal(err)
		}
		res3, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Book With Null")).
			Select(Fields(IfAbsent(FieldOf("optional_field"), "default").As("field_or_default"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res3, []map[string]any{{"field_or_default": nil}})
		coll.Doc("bookWithNull").Delete(ctx)

		// Case 4: Test different overloads.
		// ifAbsent(String, Any)
		res4, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Dune")).
			Select(Fields(IfAbsent("non_existent_field", "default_string").As("res"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res4, []map[string]any{{"res": "default_string"}})

		// ifAbsent(String, Expr)
		res5, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Dune")).
			Select(Fields(IfAbsent("non_existent_field", FieldOf("author")).As("res"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res5, []map[string]any{{"res": "Frank Herbert"}})

		// ifAbsent(Expression, Expression)
		res6, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Dune")).
			Select(Fields(IfAbsent(FieldOf("non_existent_field"), FieldOf("author")).As("res"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res6, []map[string]any{{"res": "Frank Herbert"}})
	})

	t.Run("testJoin", func(t *testing.T) {
		// Test join with a constant delimiter
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Join("tags", ", ").As("joined_tags"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["joined_tags"] != "comedy, space, adventure" {
			t.Errorf("got %v, want 'comedy, space, adventure'", res.Data()["joined_tags"])
		}

		// Test join with an expression delimiter
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Join(FieldOf("tags"), ConstantOf(" | ")).As("joined_tags"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res2.Data()["joined_tags"] != "comedy | space | adventure" {
			t.Errorf("got %v, want 'comedy | space | adventure'", res2.Data()["joined_tags"])
		}

		// Test extension method
		res3, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(FieldOf("tags").Join(" - ").As("joined_tags"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res3.Data()["joined_tags"] != "comedy - space - adventure" {
			t.Errorf("got %v, want 'comedy - space - adventure'", res3.Data()["joined_tags"])
		}
	})

	t.Run("testArraySum", func(t *testing.T) {
		_, err := coll.Doc("book4").Update(ctx, []Update{{Path: "sales", Value: []int{100, 200, 50}}})
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Fields(ArraySum("sales").As("totalSales"))).
			Limit(1).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{"totalSales": int64(350)}})
	})

	t.Run("testTimestampConversions", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(Fields(
				UnixSecondsToTimestamp(ConstantOf(int64(1741380235))).As("unixSecondsToTimestamp"),
				UnixMillisToTimestamp(ConstantOf(int64(1741380235123))).As("unixMillisToTimestamp"),
				UnixMicrosToTimestamp(ConstantOf(int64(1741380235123456))).As("unixMicrosToTimestamp"),
				TimestampToUnixSeconds(ConstantOf(time.Unix(1741380235, 123456789))).As("timestampToUnixSeconds"),
				TimestampToUnixMicros(ConstantOf(time.Unix(1741380235, 123456789))).As("timestampToUnixMicros"),
				TimestampToUnixMillis(ConstantOf(time.Unix(1741380235, 123456789))).As("timestampToUnixMillis"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := res.Data()["unixSecondsToTimestamp"].(time.Time); !got.Equal(time.Unix(1741380235, 0)) {
			t.Errorf("unixSecondsToTimestamp mismatch: got %v, want %v", got, time.Unix(1741380235, 0))
		}
		if got := res.Data()["unixMillisToTimestamp"].(time.Time); !got.Equal(time.Unix(1741380235, 123000000)) {
			t.Errorf("unixMillisToTimestamp mismatch: got %v, want %v", got, time.Unix(1741380235, 123000000))
		}
		if got := res.Data()["unixMicrosToTimestamp"].(time.Time); !got.Equal(time.Unix(1741380235, 123456000)) {
			t.Errorf("unixMicrosToTimestamp mismatch: got %v, want %v", got, time.Unix(1741380235, 123456000))
		}
		if res.Data()["timestampToUnixSeconds"] != int64(1741380235) {
			t.Errorf("timestampToUnixSeconds mismatch: got %v, want %v", res.Data()["timestampToUnixSeconds"], int64(1741380235))
		}
		if res.Data()["timestampToUnixMicros"] != int64(1741380235123456) {
			t.Errorf("timestampToUnixMicros mismatch: got %v, want %v", res.Data()["timestampToUnixMicros"], int64(1741380235123456))
		}
		if res.Data()["timestampToUnixMillis"] != int64(1741380235123) {
			t.Errorf("timestampToUnixMillis mismatch: got %v, want %v", res.Data()["timestampToUnixMillis"], int64(1741380235123))
		}
	})

	t.Run("testVectorLength", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(Fields(VectorLength(ConstantOfVector64([]float64{1.0, 2.0, 3.0})).As("vectorLength"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"vectorLength": int64(3)}})
	})

	t.Run("testStrContains", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(StringContains("title", "'s")).
			Select(Fields("title")).
			Sort(Orders(FieldOf("title").Ascending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantResults := []map[string]any{
			{"title": "The Handmaid's Tale"},
			{"title": "The Hitchhiker's Guide to the Galaxy"},
		}
		containsExactly(t, results, wantResults)
	})

	t.Run("testSubstring", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Fields(
				Substring(FieldOf("title"), ConstantOf(9), ConstantOf(2)).As("of"),
				Substring("title", 16, 5).As("Rings"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"of": "of", "Rings": "Rings"}})
	})

	t.Run("testSplitStringByStringDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Split(FieldOf("title"), " ").As("split_title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{"split_title": []any{"The", "Hitchhiker's", "Guide", "to", "the", "Galaxy"}}})

		res, err = client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(FieldOf("title").Split(" ").As("split_title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{"split_title": []any{"The", "Hitchhiker's", "Guide", "to", "the", "Galaxy"}}})
	})

	t.Run("testSplitBlobByByteArrayDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			AddFields(Selectables(ConstantOf([]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x05}).As("data"))).
			Select(Fields(Split(FieldOf("data"), ConstantOf([]byte{0x01})).As("split_data"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{
			"split_data": []any{
				[]byte{},
				[]byte{0x02, 0x03, 0x04},
				[]byte{0x05},
			},
		}})
	})

	t.Run("testSplitWithMismatchedTypesShouldFail", func(t *testing.T) {
		_, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Split(FieldOf("title"), ConstantOf([]byte{0x01})).As("mismatched_split"))).
			Execute(ctx).Results().Next()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected invalid argument error, got: %v", err)
		}
	})

	t.Run("testDistanceFunctions", func(t *testing.T) {
		v1 := Vector64{0.1, 0.1}
		v2 := Vector64{0.5, 0.8}
		res, err := client.Pipeline().Collection(coll.ID).
			Select(Fields(
				CosineDistance(ConstantOf(v1), v2).As("cosineDistance"),
				DotProduct(ConstantOf(v1), v2).As("dotProductDistance"),
				EuclideanDistance(ConstantOf(v1), v2).As("euclideanDistance"),
			)).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		wantData := map[string]any{
			"cosineDistance":     0.02560880430538015,
			"dotProductDistance": 0.13,
			"euclideanDistance":  0.806225774829855,
		}
		if !reflect.DeepEqual(data, wantData) {
			t.Errorf("got %v, want %v", data, wantData)
		}
	})

	t.Run("testNestedFields", func(t *testing.T) {
		t.Skip("Skipping functional test failure")
		gotResults, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("awards.hugo", true)).
			Select(Fields("title", "awards.hugo")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(gotResults) != 2 {
			t.Errorf("got %d results, want 2", len(gotResults))
		}

		wantResults := []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy", "awards.hugo": true},
			{"title": "Dune", "awards.hugo": true},
		}

		match1 := reflect.DeepEqual(gotResults[0].Data(), wantResults[0]) && reflect.DeepEqual(gotResults[1].Data(), wantResults[1])
		match2 := reflect.DeepEqual(gotResults[0].Data(), wantResults[1]) && reflect.DeepEqual(gotResults[1].Data(), wantResults[0])
		if !match1 && !match2 {
			t.Errorf("got:\n%#v\n%#v\nwant in any order:\n%#v", gotResults[0].Data(), gotResults[1].Data(), wantResults)
		}
	})

	t.Run("testPipelineInTransactions", func(t *testing.T) {
		p := client.Pipeline().CreateFromQuery(coll).Where(Equal("awards.hugo", true)).Select(Fields("title", "awards.hugo", "__name__"))
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *Transaction) error {
			results, err := tx.Execute(p).Results().GetAll()
			if err != nil {
				return err
			}
			if len(results) != 2 {
				return fmt.Errorf("got %d results, want 2", len(results))
			}
			return tx.Update(coll.Doc("book1"), []Update{{Path: "foo", Value: "bar"}})
		})
		if err != nil {
			t.Fatal(err)
		}

		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("foo", "bar")).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("got %d results, want 1", len(results))
		}
		// Reset
		coll.Doc("book1").Update(ctx, []Update{{Path: "foo", Value: Delete}})
	})

	t.Run("testPipelineInTransactionsWithOptions", func(t *testing.T) {
		p := client.Pipeline().CreateFromQuery(coll).
			Limit(1)
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *Transaction) error {
			results, err := tx.Execute(p).Results().GetAll()
			if err != nil {
				return err
			}
			if len(results) != 1 {
				return fmt.Errorf("got %d results, want 1", len(results))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("testRawStage", func(t *testing.T) {
		// can select fields
		res, err := client.Pipeline().Collection(coll.ID).
			RawStage("select", []any{map[string]any{
				"title":    FieldOf("title"),
				"metadata": Map(map[string]any{"author": FieldOf("author")}),
			}}).
			Sort(Orders(FieldOf("metadata.author").Ascending())).
			Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		metadata, ok := res.Data()["metadata"].(map[string]any)
		if !ok {
			t.Errorf("rawStage select mismatch metadata is not a map")
		}
		if res.Data()["title"] != "The Hitchhiker's Guide to the Galaxy" &&
			metadata["author"] != "Douglas Adams" {
			t.Errorf("rawStage select mismatch title: got: %v want: %v, metadata: got: %v want: %v", res.Data()["title"], "The Hitchhiker's Guide to the Galaxy", metadata, map[string]any{"author": "Douglas Adams"})
		}

		// can add fields
		res, err = client.Pipeline().Collection(coll.ID).
			Sort(Orders(FieldOf("author").Ascending())).
			Limit(1).
			Select(Fields("title", "author")).
			RawStage("add_fields", []any{map[string]any{
				"display": StringConcat(FieldOf("title"), " - ", FieldOf("author")),
			}}).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		if res.Data()["title"] != "The Hitchhiker's Guide to the Galaxy" &&
			res.Data()["author"] != "Douglas Adams" &&
			res.Data()["display"] != "The Hitchhiker's Guide to the Galaxy - Douglas Adams" {
			t.Errorf("rawStage select mismatch title: got: %v want: %v, author: got: %v want: %v, display: got: %v want: %v", res.Data()["title"], "The Hitchhiker's Guide to the Galaxy", res.Data()["author"], "Douglas Adams", res.Data()["display"], "The Hitchhiker's Guide to the Galaxy - Douglas Adams")
		}

		// can filter with where
		res, err = client.Pipeline().Collection(coll.ID).
			Select(Fields("title", "author")).
			RawStage("where", []any{Equal(FieldOf("author"), "Douglas Adams")}).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		if res.Data()["title"] != "The Hitchhiker's Guide to the Galaxy" &&
			res.Data()["author"] != "Douglas Adams" {
			t.Errorf("rawStage where mismatch title: got: %v want: %v, author: got: %v want: %v", res.Data()["title"], "The Hitchhiker's Guide to the Galaxy", res.Data()["author"], "Douglas Adams")
		}

		// can limit, offset, and sort
		res, err = client.Pipeline().Collection(coll.ID).
			Select(Fields("title", "author")).
			RawStage("sort", []any{map[string]any{
				"direction":  "ascending",
				"expression": FieldOf("author"),
			}}).
			RawStage("offset", []any{3}).
			RawStage("limit", []any{1}).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		if res.Data()["title"] != "Crime and Punishment" &&
			res.Data()["author"] != "Fyodor Dostoevsky" {
			t.Errorf("rawStage sort, offset, limit mismatch title: got: %v want: %v, author: got: %v want: %v", res.Data()["title"], "Crime and Punishment", res.Data()["author"], "Fyodor Dostoevsky")
		}

		// can perform aggregate query
		res, err = client.Pipeline().Collection(coll.ID).
			Select(Fields("title", "author", "rating")).
			RawStage("aggregate", []any{map[string]any{
				"averageRating": Average("rating"),
			}, map[string]any{}}).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		if math.Abs(res.Data()["averageRating"].(float64)-4.31) > 0.00001 {
			t.Errorf("rawStage aggregate mismatch averageRating: got: %v want: %v", res.Data()["averageRating"], 4.31)
		}

		// can perform distinct query
		results, err := client.Pipeline().Collection(coll.ID).
			Select(Fields("title", "author", "rating")).
			RawStage("distinct", []any{map[string]any{
				"rating": FieldOf("rating"),
			}}).
			Sort(Orders(FieldOf("rating").Descending())).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		wantRatings := []any{4.7, 4.6, 4.5, 4.3, 4.2, 4.1, 4.0, nil}
		for i, res := range results {
			if res.Data()["rating"] != wantRatings[i] {
				t.Errorf("rawStage distinct mismatch rating: got: %v want: %v", res.Data()["rating"], wantRatings[i])
			}
		}

		// // can perform FindNearest query
		results, err = client.Pipeline().Collection(coll.ID).
			RawStage("find_nearest",
				[]any{FieldOf("embedding"), ConstantOfVector64([]float64{10.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}), "euclidean"},
				RawOptions{
					"distance_field": FieldOf("computedDistance"),
					"limit":          2,
				}).
			Select(Fields("title", "computedDistance")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 2 {
			t.Errorf("rawStage findNearest mismatch results: got: %v want: %v", len(results), 2)
		}

		if results[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("rawStage findNearest mismatch results[0] title: got: %v want: %v", results[0].Data()["title"], "The Hitchhiker's Guide to the Galaxy")
		}
		if math.Abs(results[0].Data()["computedDistance"].(float64)-1.0) > 0.00001 {
			t.Errorf("rawStage findNearest mismatch results[0] computedDistance: got: %v want: %v", results[0].Data()["computedDistance"], 10.0)
		}
		if results[1].Data()["title"] != "One Hundred Years of Solitude" {
			t.Errorf("rawStage findNearest mismatch results[1] title: got: %v want: %v", results[1].Data()["title"], "One Hundred Years of Solitude")
		}
		if math.Abs(results[1].Data()["computedDistance"].(float64)-12.041594578792296) > 0.00001 {
			t.Errorf("rawStage findNearest mismatch results[1] computedDistance: got: %v want: %v", results[1].Data()["computedDistance"], 12.041594578792296)
		}

	})

	t.Run("testReplaceWith", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).ReplaceWith("awards").Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"hugo": true, "nebula": false}})

		res, err = client.Pipeline().CreateFromQuery(coll).Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			ReplaceWith(
				Map(map[string]any{
					"foo": "bar",
					"baz": Map(map[string]any{"title": FieldOf("title")}),
				})).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"foo": "bar", "baz": map[string]any{"title": "The Hitchhiker's Guide to the Galaxy"}}})
	})

	t.Run("sample", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Sample(ByDocuments(2)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	t.Run("testSamplePercentage", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Sample(ByPercentage(0.6)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) == 0 {
			t.Errorf("got %d results, want >0", len(results))
		}
	})

	t.Run("testUnion", func(t *testing.T) {
		p1 := client.Pipeline().CreateFromQuery(coll)
		p2 := client.Pipeline().CreateFromQuery(coll)
		results, err := p1.Union(p2).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 22 {
			t.Errorf("got %d results, want 22", len(results))
		}
	})

	t.Run("testUnnest", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag").Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	t.Run("testUnnestWithIndexField", func(t *testing.T) {
		t.Skip("Flaky")
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag", WithUnnestIndexField("tagsIndex")).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	t.Run("testUnnestWithExpr", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Unnest(Array(1, 2, 3).As("copy")).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	t.Run("testPaginationWithStartAfter", func(t *testing.T) {
		paginationCollection := client.Collection(collectionIDs.New())
		docs := []map[string]any{
			{"order": 1},
			{"order": 2},
			{"order": 3},
			{"order": 4},
		}
		for i, doc := range docs {
			h.mustCreate(paginationCollection.Doc(fmt.Sprintf("doc%d", i+1)), doc)
		}

		p1 := client.Pipeline().CreateFromQuery(paginationCollection.OrderBy("order", Asc).Limit(2))
		results1, err := p1.Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want1 := []map[string]any{
			{"order": int64(1)},
			{"order": int64(2)},
		}
		containsExactly(t, results1, want1)

		lastResult := results1[len(results1)-1]
		p2 := client.Pipeline().CreateFromQuery(paginationCollection.OrderBy("order", Asc).StartAfter(lastResult.Data()["order"]))
		results2, err := p2.Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want2 := []map[string]any{
			{"order": int64(3)},
			{"order": int64(4)},
		}
		containsExactly(t, results2, want2)
	})

	t.Run("testDocumentsAsSource", func(t *testing.T) {
		results, err := client.Pipeline().
			Documents([]*DocumentRef{coll.Doc("book1"), coll.Doc("book2"), coll.Doc("book3")}).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	t.Run("testCollectionGroupAsSource", func(t *testing.T) {
		subcollectionID := uuid.New().String()
		docs := []map[string]any{
			{"order": 1},
			{"order": 2},
		}
		coll.Doc("book1").Collection(subcollectionID).Add(ctx, docs[0])
		coll.Doc("book2").Collection(subcollectionID).Add(ctx, docs[1])
		results, err := client.Pipeline().
			CollectionGroup(subcollectionID).
			Sort(Orders(FieldOf("order").Ascending())).
			Execute(ctx).
			Results().
			GetAll()
		if err != nil {
			t.Fatal(err)
		}

		wantResults := []map[string]any{
			{"order": int64(1)},
			{"order": int64(2)},
		}
		isEqualTo(t, results, wantResults)
	})

	t.Run("testDatabaseAsSource", func(t *testing.T) {
		randomID := rand.IntN(10000)
		docs := []map[string]any{
			{"order": 1, "randomId": randomID},
			{"order": 2, "randomId": randomID},
		}
		coll.Doc("book1").Collection("sub").Add(ctx, docs[0])
		coll.Doc("book2").Collection("sub").Add(ctx, docs[1])
		results, err := client.Pipeline().
			Database().
			Where(Equal("randomId", randomID)).
			Sort(Orders(FieldOf("order").Ascending())).
			Execute(ctx).
			Results().
			GetAll()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("results[0]: %v, results[1]: %v\n", results[0].Data(), results[1].Data())
		wantResults := []map[string]any{
			{"order": int64(1), "randomId": int64(randomID)},
			{"order": int64(2), "randomId": int64(randomID)},
		}
		isEqualTo(t, results, wantResults)
	})

	t.Run("testFindNearest", func(t *testing.T) {
		v := Vector64{10.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}
		res, err := client.Pipeline().Collection(coll.ID).
			FindNearest("embedding", v,
				PipelineDistanceMeasureEuclidean, RawOptions{
					"limit":          2,
					"distance_field": "computedDistance",
				}).
			Select(Fields("title", "computedDistance")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Errorf("got %d results, want 2", len(res))
		}
		if res[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("res[0].Data()[\"title\"] mismatch got %v", res[0].Data()["title"])
		}
		if res[1].Data()["title"] != "One Hundred Years of Solitude" {
			t.Errorf("res[1].Data()[\"title\"] mismatch got %v", res[1].Data()["title"])
		}
		if math.Abs(res[0].Data()["computedDistance"].(float64)-1.0) > 0.00001 {
			t.Errorf("res[0].Data()[\"computedDistance\"] mismatch got %v", res[0].Data()["computedDistance"])
		}
		if math.Abs(res[1].Data()["computedDistance"].(float64)-12.041594578792296) > 0.00001 {
			t.Errorf("res[1].Data()[\"computedDistance\"] mismatch got %v", res[1].Data()["computedDistance"])
		}
	})

	t.Run("testExplain", func(t *testing.T) {
		snap := client.Pipeline().CreateFromQuery(coll).
			Sort(Orders(FieldOf("__name__").Ascending())).
			Execute(ctx, WithExplainMode(ExplainModeAnalyze))
		pr, _ := snap.Results().GetAll()
		if len(pr) == 0 {
			t.Fatal("no results")
		}
		stats := snap.ExplainStats()
		if stats == nil {
			t.Fatal("ExplainStats is nil")
		}

		snap = client.Pipeline().CreateFromQuery(coll).
			Sort(Orders(FieldOf("__name__").Ascending())).
			Execute(ctx)
		pr, _ = snap.Results().GetAll()
		if len(pr) == 0 {
			t.Fatal("no results")
		}
		stats = snap.ExplainStats()
		if stats != nil {
			t.Fatal("ExplainStats is not nil")
		}
	})

	t.Run("testArrayFirst", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayFirst("tags").Equal("adventure")).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayFirst().Equal("adventure")).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// Test with empty/null/non-existent arrays
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"empty": []any{}, "nullval": ConstantOfNull()})).
			Select(Fields(
				ArrayFirst("empty").As("emptyResult"),
				ArrayFirst("nullval").As("nullResult"),
				ArrayFirst("nonExistent").As("absentResult"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"nullResult": nil, "absentResult": nil}})

	})

	t.Run("testArrayFirstN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayFirstN("tags", 2).Equal([]string{"adventure", "magic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayFirstN(4).Equal([]string{"adventure", "magic", "epic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// Test with empty/null/non-existent arrays
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"empty": []any{}, "nullval": ConstantOfNull()})).
			Select(Fields(
				ArrayFirstN("empty", 2).As("emptyResult"),
				ArrayFirstN("nullval", 2).As("nullResult"),
				ArrayFirstN("nonExistent", 2).As("absentResult"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"emptyResult": []any{}, "nullResult": nil, "absentResult": nil}})
	})

	t.Run("testArrayLast", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayLast("tags").Equal("epic")).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayLast().Equal("epic")).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// Test with empty/null/non-existent arrays
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"empty": []any{}, "nullval": ConstantOfNull()})).
			Select(Fields(
				ArrayLast("empty").As("emptyResult"),
				ArrayLast("nullval").As("nullResult"),
				ArrayLast("nonExistent").As("absentResult"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		// no emptyResult as arrayLast returns UNSET for empty arrays
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"nullResult": nil, "absentResult": nil}})
	})

	t.Run("testArrayLastN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayLastN("tags", 2).Equal([]string{"magic", "epic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayLastN(4).Equal([]string{"adventure", "magic", "epic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// Test with empty/null/non-existent arrays
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"empty": []any{}, "nullval": ConstantOfNull()})).
			Select(Fields(
				ArrayLastN("empty", 2).As("emptyResult"),
				ArrayLastN("nullval", 2).As("nullResult"),
				ArrayLastN("nonExistent", 2).As("absentResult"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"emptyResult": []any{}, "nullResult": nil, "absentResult": nil}})
	})

	t.Run("testArrayMinimum", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayMinimum("tags").Equal("adventure")).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantRes1 := []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy"},
			{"title": "The Lord of the Rings"},
		}
		isEqualTo(t, results, wantRes1)

		results, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayMinimum().Equal("adventure")).
			Select(Fields("title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantRes2 := []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy"},
			{"title": "The Lord of the Rings"},
		}
		isEqualTo(t, results, wantRes2)

		// Test with empty/null/non-existent arrays
		results, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"empty": []any{}, "nullval": ConstantOfNull(), "mixed": []any{1, "2", 3, "10"}})).
			Select(Fields(
				ArrayMinimum("empty").As("emptyResult"),
				ArrayMinimum("nullval").As("nullResult"),
				ArrayMinimum("nonExistent").As("absentResult"),
				ArrayMinimum("mixed").As("mixedResult"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		wantRes3 := []map[string]any{
			{"emptyResult": nil, "nullResult": nil, "absentResult": nil, "mixedResult": int64(1)},
		}
		isEqualTo(t, results, wantRes3)
	})

	t.Run("testArrayMinimumN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayMinimumN("tags", 2).Equal([]string{"adventure", "epic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayMinimumN(4).Equal([]string{"adventure", "epic", "magic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})
	})

	t.Run("testArrayMaximum", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayMaximum("tags").Equal("magic")).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayMaximum().Equal("magic")).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// Test with empty/null/non-existent and mixed types
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(
				Map(map[string]any{
					"empty":   []any{},
					"nullval": ConstantOfNull(),
					"mixed":   []any{1, "2", 3, "10"},
				}),
			).
			Select(Fields(
				ArrayMaximum("empty").As("emptyResult"),
				ArrayMaximum("nullval").As("nullResult"),
				ArrayMaximum("nonExistent").As("absentResult"),
				ArrayMaximum("mixed").As("mixedResult"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{
				"emptyResult":  nil,
				"nullResult":   nil,
				"absentResult": nil,
				"mixedResult":  "2",
			},
		}
		isEqualTo(t, []*PipelineResult{res}, want)
	})

	t.Run("testArrayMaximumN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayMaximumN("tags", 2), []string{"magic", "epic"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("tags").ArrayMaximumN(4).Equal([]string{"magic", "epic", "adventure"})).
			Select(Fields("title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		isEqualTo(t, []*PipelineResult{res}, []map[string]any{{"title": "The Lord of the Rings"}})
	})

	t.Run("testArrayIndexOf", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Fields(
				ArrayIndexOf("tags", "adventure").As("indexFirst"),
				ArrayIndexOf(FieldOf("tags"), "magic").As("indexSecond"),
				FieldOf("tags").ArrayIndexOf("epic").As("indexLast"),
				ArrayIndexOf("tags", "nonexistent").As("indexNone"),
				ArrayIndexOf("empty", "anything").As("indexEmpty"),
			)).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{
				"indexFirst":  int64(0),
				"indexSecond": int64(1),
				"indexLast":   int64(2),
				"indexNone":   int64(-1),
				"indexEmpty":  nil,
			},
		}
		isEqualTo(t, []*PipelineResult{res}, want)

		// Test with duplicate values
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"arr": []int{1, 2, 3, 2, 1}})).
			Select(Fields(ArrayIndexOf("arr", 2).As("firstIndex"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want = []map[string]any{
			{"firstIndex": int64(1)},
		}
		isEqualTo(t, []*PipelineResult{res}, want)

		// Test with null value
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"arr": []any{1, nil, 3, 2, 1}, "nullArr": nil})).
			Select(Fields(
				ArrayIndexOf("arr", nil).As("nullIndex"),
				ArrayIndexOf("nullArr", nil).As("nullIndexNull"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want = []map[string]any{
			{
				"nullIndex":     int64(1),
				"nullIndexNull": nil,
			},
		}
		isEqualTo(t, []*PipelineResult{res}, want)
	})

	t.Run("testArrayIndexOfAll", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Fields(
				ArrayIndexOfAll("tags", "adventure").As("indicesFirst"),
				ArrayIndexOfAll(FieldOf("tags"), "magic").As("indicesSecond"),
				FieldOf("tags").ArrayIndexOfAll("epic").As("indicesLast"),
				ArrayIndexOfAll("tags", "nonexistent").As("indicesNone"),
			)).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{
				"indicesFirst":  []any{int64(0)},
				"indicesSecond": []any{int64(1)},
				"indicesLast":   []any{int64(2)},
				"indicesNone":   []any{},
			},
		}
		isEqualTo(t, []*PipelineResult{res}, want)

		// Test with duplicate values
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"arr": []int{1, 2, 3, 2, 1}})).
			Select(Fields(ArrayIndexOfAll("arr", 2).As("indices"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want = []map[string]any{
			{"indices": []any{int64(1), int64(3)}},
		}
		isEqualTo(t, []*PipelineResult{res}, want)

		// Test with null values
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"arr": []any{1, nil, 3, nil, 1}, "nullArr": nil})).
			Select(Fields(
				ArrayIndexOfAll("arr", nil).As("indices"),
				ArrayIndexOfAll("nullArr", nil).As("indicesNull"),
				ArrayIndexOfAll("nonExistentArray", nil).As("indicesNonExistent"),
			)).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		want = []map[string]any{
			{
				"indices":            []any{int64(1), int64(3)},
				"indicesNull":        nil,
				"indicesNonExistent": nil,
			},
		}
		isEqualTo(t, []*PipelineResult{res}, want)
	})

	t.Run("testSplitStringByExpressionDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Split(FieldOf("title"), ConstantOf(" ")).As("split_title"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["split_title"].([]any)
		if !reflect.DeepEqual(got, []any{"The", "Hitchhiker's", "Guide", "to", "the", "Galaxy"}) {
			t.Errorf("split string by expression delimiter mismatch: %v", got)
		}

		res, err = client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(FieldOf("title").Split(ConstantOf(" ")).As("split_title"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got = res.Data()["split_title"].([]any)
		if !reflect.DeepEqual(got, []any{"The", "Hitchhiker's", "Guide", "to", "the", "Galaxy"}) {
			t.Errorf("split string by expression delimiter mismatch: %v", got)
		}
	})

	t.Run("testOptions", func(t *testing.T) {
		if useEmulator {
			t.Skip("Certain options are not supported against the emulator.")
		}

		_, err := client.Pipeline().Collection(
			"/k",
			WithForceIndex("title"),
		).FindNearest(
			"topicVectors",
			[]float64{1.0, 2.0, 3.0},
			PipelineDistanceMeasureCosine,
			RawOptions{
				"limit":          10,
				"distance_field": "distance",
			},
		).Aggregate(Accumulators(Average("rating").As("avg_rating")),
			WithAggregateGroups("genre"),
			RawOptions{"test_option": "test_value"}).Execute(ctx,
			WithIndexMode("recommended"),
			WithExplainMode(ExplainModeAnalyze),
		).Results().GetAll()

		fmt.Println(err)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("testErrorHandling", func(t *testing.T) {
		ps := client.Pipeline().Collection(coll.ID).
			RawStage("invalidStage", nil).
			Execute(ctx)

		_, err := ps.Results().Next()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("got error code %v, want %v", status.Code(err), codes.InvalidArgument)
		}
	})

	t.Run("testType", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("author", "Douglas Adams")).
			Limit(1).
			Select(Fields(
				Type("title").As("string_type"),
				Type("published").As("number_type"),
				Type(FieldOf("awards").MapGet("hugo")).As("boolean_type"),
				Type(ConstantOfNull()).As("null_type"),
				Type("embedding").As("vector_type"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		containsExactly(t, res, []map[string]any{{"string_type": "string", "number_type": "int64", "boolean_type": "boolean", "null_type": "null", "vector_type": "vector"}})
	})

	t.Run("testExplainWithError", func(t *testing.T) {
		if useEmulator {
			t.Skip("Explain with error is not supported against the emulator")
		}
		pipeline := client.Pipeline().Collection(coll.ID).Sort(Orders(Ascending(FieldOf("rating"))))
		snap := pipeline.Execute(ctx,
			WithExplainMode(ExplainModeAnalyze),
			RawOptions{"memory_limit": 1},
		)

		_, err := snap.Results().GetAll()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Errorf("got error code %v, want %v", status.Code(err), codes.ResourceExhausted)
		}
	})

	t.Run("testUnnestWithExpr", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(FieldOf("title"), "The Hitchhiker's Guide to the Galaxy")).
			Unnest(Array(int64(1), int64(2), int64(3)).As("copy")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("testUnnestWithExpr size mismatch: got %d, want 3", len(results))
		}
		for i, res := range results {
			if res.Data()["copy"] != int64(i+1) {
				t.Errorf("testUnnestWithExpr mismatch at %d: got %v", i, res.Data()["copy"])
			}
		}
	})

	t.Run("testUnnestWithIndexField", func(t *testing.T) {
		t.Skip("Flaky")
		opts := WithUnnestIndexField("tagsIndex")
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag", opts).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("testUnnestWithIndexField size mismatch")
		}
		for i, res := range results {
			if res.Data()["tagsIndex"] != int64(i) {
				t.Errorf("testUnnestWithIndexField tagsIndex mismatch: got %v", res.Data()["tagsIndex"])
			}
		}
	})

	t.Run("testArrayLastIndexOf", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Fields(
				ArrayLastIndexOf("tags", "adventure").As("lastIndexFirst"),
				ArrayLastIndexOf(FieldOf("tags"), "magic").As("lastIndexSecond"),
				FieldOf("tags").ArrayLastIndexOf("epic").As("lastIndexLast"),
				ArrayLastIndexOf("tags", "nonexistent").As("lastIndexNone"),
				ArrayLastIndexOf("empty", "anything").As("lastIndexEmpty"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{
			{
				"lastIndexFirst":  int64(0),
				"lastIndexSecond": int64(1),
				"lastIndexLast":   int64(2),
				"lastIndexNone":   int64(-1),
				"lastIndexEmpty":  nil,
			},
		}
		isEqualTo(t, results, want)

		// Test with duplicate values
		results, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"arr": []any{1, 2, 3, 2, 1}})).
			Select(Fields(ArrayLastIndexOf("arr", 2).As("lastIndex"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want = []map[string]any{
			{"lastIndex": int64(3)},
		}
		isEqualTo(t, results, want)

		// Test with null value
		results, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			ReplaceWith(Map(map[string]any{"arr": []any{1, nil, 3, 2, 1}, "nullArr": nil})).
			Select(Fields(
				ArrayLastIndexOf("arr", nil).As("nullIndex"),
				ArrayLastIndexOf("nullArr", nil).As("nullIndexNull"),
			)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		want = []map[string]any{
			{"nullIndex": int64(1), "nullIndexNull": nil},
		}
		isEqualTo(t, results, want)
	})

	t.Run("testArrayAggAccumulatorsWithInstanceMethod", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(FieldOf("published").GreaterThan(0)).
			Sort(Orders(FieldOf("published").Ascending())).
			Aggregate(Accumulators(FieldOf("rating").ArrayAgg().As("allRatings"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("got %d docs, want 1", len(results))
		}
		got := results[0].Data()["allRatings"].([]any)
		want := []any{4.5, 4.3, 4.0, 4.2, 4.7, 4.2, 4.6, 4.3, 4.2, 4.1}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("allRatings: %s", diff)
		}
	})

	t.Run("testTruncWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("title").Equal("Pride and Prejudice")).
			Limit(1).
			Select(Fields(
				FieldOf("rating").Trunc().As("truncatedRating"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["truncatedRating"].(float64) != 4.0 {
			t.Errorf("truncatedRating: got %v, want 4.0", data["truncatedRating"])
		}
	})

	t.Run("testTruncToPrecisionWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(Fields(
				ConstantOf(4.123456).TruncToPrecision(0).As("p0"),
				ConstantOf(4.123456).TruncToPrecision(1).As("p1"),
				ConstantOf(4.123456).TruncToPrecision(ConstantOf(2)).As("p2"),
				ConstantOf(4.123456).TruncToPrecision(4).As("p4"),
			)).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["p0"].(float64) != 4.0 {
			t.Errorf("p0: got %v, want 4.0", data["p0"])
		}
		if data["p1"].(float64) != 4.1 {
			t.Errorf("p1: got %v, want 4.1", data["p1"])
		}
		if data["p2"].(float64) != 4.12 {
			t.Errorf("p2: got %v, want 4.12", data["p2"])
		}
		if data["p4"].(float64) != 4.1234 {
			t.Errorf("p4: got %v, want 4.1234", data["p4"])
		}
	})

	t.Run("testSplitStringFieldByExpressionDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Split("title", ConstantOf(" ")).As("split_title"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["split_title"].([]any)
		want := []any{"The", "Hitchhiker's", "Guide", "to", "the", "Galaxy"}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("split_title: %s", diff)
		}
	})

	t.Run("testSplitStringFieldByStringDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Fields(Split("title", " ").As("split_title"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["split_title"].([]any)
		want := []any{"The", "Hitchhiker's", "Guide", "to", "the", "Galaxy"}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("split_title: %s", diff)
		}
	})

	t.Run("disallowDuplicateAliases", func(t *testing.T) {
		t.Skip("Skipping functional test failure")
		t.Skip("Skipping functional test failure")
		t.Run("disallowDuplicateAliasesInSelect", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).Select(Fields("title", FieldOf("author").As("title")))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesInAddFields", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).AddFields(Selectables(FieldOf("title").As("dup"), FieldOf("author").As("dup")))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesInAggregate", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).
				Aggregate(Accumulators(CountAll().As("dup"), Average("rating").As("dup")))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesInDistinct", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).Distinct(Fields(FieldOf("genre").As("dup"), FieldOf("author").As("dup")))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesAcrossStages", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).
				Select(Fields(FieldOf("title").As("title_dup"))).
				AddFields(Selectables(FieldOf("author").As("author_dup"))).
				Distinct(Fields(FieldOf("genre").As("genre_dup"))).
				Select(Fields(FieldOf("title_dup").As("final_dup"), FieldOf("author_dup").As("final_dup")))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
	})
}

func containsExactly(t *testing.T, gotResults []*PipelineResult, want []map[string]any) {
	t.Helper()

	if len(gotResults) != len(want) {
		t.Errorf("got %d results, want %d\ngot:  %v\nwant: %v", len(gotResults), len(want), gotResults, want)
		return
	}

	matched := make([]bool, len(gotResults))
	for _, w := range want {
		found := false
		for j, g := range gotResults {
			if !matched[j] && testutil.Diff(g.Data(), w) == "" {
				matched[j] = true
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected element:\n%v\n\nremaining unmatched actual elements:", w)
			for j, g := range gotResults {
				if !matched[j] {
					t.Errorf("  %v", g.Data())
				}
			}
			return
		}
	}
}

func isEqualTo(t *testing.T, gotResults []*PipelineResult, want []map[string]any) {
	t.Helper()

	if len(gotResults) != len(want) {
		t.Errorf("got %d results, want %d\ngot:  %v\nwant: %v", len(gotResults), len(want), gotResults, want)
		return
	}

	for i, w := range want {
		g := gotResults[i].Data()
		if diff := testutil.Diff(g, w); diff != "" {
			t.Errorf("mismatch at index %d:\nwant+ got-\n%s", i, diff)
		}
	}
}
