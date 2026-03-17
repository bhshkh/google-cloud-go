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
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"google.golang.org/genproto/googleapis/type/latlng"
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
			"empty":     []string{},
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

	// testAllDataTypes
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
			Select(
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
			)

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
		// Array comparison might be tricky due to time.Time vs Timestamp
		// but Go client usually returns time.Time.
	})

	// testResultMetadata
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

	// testResultIsEqual
	t.Run("testResultIsEqual", func(t *testing.T) {
		p := client.Pipeline().Collection(coll.ID).Sort(FieldOf("title").Ascending())
		res1, err := p.Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		res2, err := p.Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		res3, err := p.Offset(1).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}

		// Go doesn't have Equals on PipelineResult, so we compare fields
		if res1.Ref().Path != res2.Ref().Path || !reflect.DeepEqual(res1.Data(), res2.Data()) {
			t.Error("res1 should be equal to res2")
		}
		if res1.Ref().Path == res3.Ref().Path {
			t.Error("res1 should not be equal to res3")
		}
	})

	// testEmptyResultMetadata
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

	// testAggregateResultMetadata
	t.Run("testAggregateResultMetadata", func(t *testing.T) {
		snap := client.Pipeline().Collection(coll.ID).Aggregate(CountAll().As("count")).Execute(ctx)
		res, err := snap.Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.CreateTime() != nil {
			t.Errorf("Aggregate result should have zero CreateTime, got %v", res.CreateTime())
		}
		if res.UpdateTime() != nil {
			t.Errorf("Aggregate result should have zero UpdateTime, got %v", res.UpdateTime())
		}
	})

	// testAggregates
	t.Run("testAggregates", func(t *testing.T) {
		res1, err := client.Pipeline().Collection(coll.ID).Aggregate(CountAll().As("count")).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res1.Data()["count"] != int64(11) {
			t.Errorf("got count %v, want 11", res1.Data()["count"])
		}

		res2, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("genre", "Science Fiction")).
			Aggregate(
				CountAll().As("count"),
				Average("rating").As("avg_rating"),
				FieldOf("rating").Maximum().As("max_rating"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res2.Data()
		if data["count"] != int64(2) {
			t.Errorf("got count %v, want 2", data["count"])
		}
		if math.Abs(data["avg_rating"].(float64)-4.4) > 0.001 {
			t.Errorf("got avg_rating %v, want 4.4", data["avg_rating"])
		}
		if data["max_rating"] != 4.6 {
			t.Errorf("got max_rating %v, want 4.6", data["max_rating"])
		}
	})

	// testMoreAggregates
	t.Run("testMoreAggregates", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Aggregate(
			Sum("rating").As("sum_rating"),
			Count("rating").As("count_rating"),
			CountDistinct("genre").As("distinct_genres"),
		).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if math.Abs(data["sum_rating"].(float64)-43.1) > 0.001 {
			t.Errorf("got sum_rating %v, want 43.1", data["sum_rating"])
		}
		if data["count_rating"] != int64(10) {
			t.Errorf("got count_rating %v, want 10", data["count_rating"])
		}
		if data["distinct_genres"] != int64(8) {
			t.Errorf("got distinct_genres %v, want 8", data["distinct_genres"])
		}
	})

	// testCountIfAggregate
	t.Run("testCountIfAggregate", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Aggregate(
			CountIf(GreaterThan("rating", 4.3)).As("count"),
		).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["count"] != int64(3) {
			t.Errorf("got count %v, want 3", res.Data()["count"])
		}
	})

	// testGroupBysWithoutAccumulators
	t.Run("testGroupBysWithoutAccumulators", func(t *testing.T) {
		p := client.Pipeline().CreateFromQuery(coll).Where(LessThan("published", 1900)).
			AggregateWithSpec(NewAggregateSpec().WithGroups("genre"))
		_, err := p.Execute(ctx).Results().Next()
		if err == nil {
			t.Error("Expected error for Aggregate without accumulators")
		} else if !strings.Contains(err.Error(), "requires at least one accumulator") {
			t.Errorf("Expected error containing 'requires at least one accumulator', got: %v", err)
		}
	})

	// testDistinct
	t.Run("testDistinct", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1900)).
			Distinct(FieldOf("genre").ToLower().As("lower_genre")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		var genres []string
		for _, res := range results {
			genres = append(genres, res.Data()["lower_genre"].(string))
		}
		sort.Strings(genres)
		want := []string{"psychological thriller", "romance"}
		if !reflect.DeepEqual(genres, want) {
			t.Errorf("got %v, want %v", genres, want)
		}
	})

	// testGroupBysAndAggregate
	t.Run("testGroupBysAndAggregate", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1984)).
			AggregateWithSpec(
				NewAggregateSpec(Average("rating").As("avg_rating")).WithGroups("genre"),
			).
			Where(GreaterThan("avg_rating", 4.3)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
		// Expect: Fantasy (4.7), Romance (4.5), Science Fiction (4.4)
	})

	// testMinMax
	t.Run("testMinMax", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).Aggregate(
			CountAll().As("count"),
			FieldOf("rating").Maximum().As("max_rating"),
			FieldOf("published").Minimum().As("min_published"),
		).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["count"] != int64(11) {
			t.Errorf("got count %v, want 11", data["count"])
		}
		if data["max_rating"] != 4.7 {
			t.Errorf("got max_rating %v, want 4.7", data["max_rating"])
		}
		if data["min_published"] != int64(1813) {
			t.Errorf("got min_published %v, want 1813", data["min_published"])
		}
	})

	// testFirstAndLastAccumulators
	t.Run("testFirstAndLastAccumulators", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(GreaterThan("published", 0)).
			Sort(FieldOf("published").Ascending()).
			Aggregate(
				First("rating").As("firstBookRating"),
				First("title").As("firstBookTitle"),
				Last("rating").As("lastBookRating"),
				Last("title").As("lastBookTitle"),
			).Execute(ctx).Results().Next()
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

	// testFirstAndLastAccumulatorsWithInstanceMethod
	t.Run("testFirstAndLastAccumulatorsWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(GreaterThan("published", 0)).
			Sort(FieldOf("published").Ascending()).
			Aggregate(
				FieldOf("rating").First().As("firstBookRating"),
				FieldOf("title").First().As("firstBookTitle"),
				FieldOf("rating").Last().As("lastBookRating"),
				FieldOf("title").Last().As("lastBookTitle"),
			).Execute(ctx).Results().Next()
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

	// testArrayAggAccumulators
	t.Run("testArrayAggAccumulators", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(GreaterThan("published", 0)).
			Sort(FieldOf("published").Ascending()).
			Aggregate(ArrayAgg("rating").As("allRatings")).
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

	// testArrayAggDistinctAccumulators
	t.Run("testArrayAggDistinctAccumulators", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(GreaterThan("published", 0)).
			Aggregate(ArrayAggDistinct("rating").As("allDistinctRatings")).
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

	// testArrayAggDistinctAccumulatorsWithInstanceMethod
	t.Run("testArrayAggDistinctAccumulatorsWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("published").GreaterThan(0)).
			Aggregate(FieldOf("rating").ArrayAggDistinct().As("allDistinctRatings")).
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

	// selectSpecificFields
	t.Run("selectSpecificFields", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Select("title", "author").
			Sort(FieldOf("author").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 11 {
			t.Errorf("got %d results, want 11", len(results))
		}
		if results[0].Data()["author"] != "Douglas Adams" {
			t.Errorf("Expected first author Douglas Adams, got %v", results[0].Data()["author"])
		}
	})

	// addAndRemoveFields
	t.Run("addAndRemoveFields", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(NotEqual("author", "Timestamp Author")).
			AddFields(
				FieldOf("author").StringConcat("_", FieldOf("title")).As("author_title"),
				FieldOf("title").StringConcat("_", FieldOf("author")).As("title_author"),
			).
			RemoveFields("title_author", "tags", "awards", "rating", "title", "embedding", "cost").
			RemoveFields("published", "genre", "nestedField").
			Sort(FieldOf("author_title").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 10 {
			t.Errorf("got %d results, want 10", len(results))
		}
		data := results[0].Data()
		if len(data) != 2 {
			t.Errorf("Expected 2 fields, got %d: %v", len(data), data)
		}
		if data["author_title"] != "Douglas Adams_The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("mismatch: %v", data)
		}
	})

	// whereByMultipleConditions
	t.Run("whereByMultipleConditions", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(And(GreaterThan("rating", 4.5), Equal("genre", "Science Fiction"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].Ref().ID != "book10" {
			t.Errorf("Expected book10, got %v", results)
		}
	})

	// whereByOrCondition
	t.Run("whereByOrCondition", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Or(Equal("genre", "Romance"), Equal("genre", "Dystopian"))).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	// testPipelineWithOffsetAndLimit
	t.Run("testPipelineWithOffsetAndLimit", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Sort(FieldOf("author").Ascending()).
			Offset(5).
			Limit(3).
			Select("title", "author").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
		if results[0].Data()["title"] != "1984" {
			t.Errorf("Expected 1984, got %v", results[0].Data()["title"])
		}
	})

	// testArrayContains
	t.Run("testArrayContains", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(ArrayContains("tags", "comedy")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].Ref().ID != "book1" {
			t.Errorf("Expected book1, got %v", results)
		}
	})

	// testArrayContainsAny
	t.Run("testArrayContainsAny", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(ArrayContainsAny("tags", []string{"comedy", "classic"})).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testArrayContainsAll
	t.Run("testArrayContainsAll", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(ArrayContainsAll("tags", []string{"adventure", "magic"})).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].Data()["title"] != "The Lord of the Rings" {
			t.Errorf("Expected LotR, got %v", results)
		}
	})

	// testArrayLength
	t.Run("testArrayLength", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			AddFields(FieldOf("tags").ArrayLength().As("tagsCount")).
			Where(Equal("tagsCount", 3)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 10 {
			t.Errorf("got %d results, want 10", len(results))
		}
	})

	// testArrayConcat
	t.Run("testArrayConcat", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("author").Equal("Frank Herbert")).
			AddFields(FieldOf("tags").ArrayConcat(Array("classic", "scifi")).As("modifiedTags")).
			Select("modifiedTags").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("got %d docs, want 1", len(results))
		}
		got := results[0].Data()["modifiedTags"].([]interface{})
		want := []interface{}{"politics", "desert", "ecology", "classic", "scifi"}
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("modifiedTags: %s", diff)
		}
	})

	// testStrConcat
	t.Run("testStrConcat", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("author", "Douglas Adams")).
			AddFields(FieldOf("author").StringConcat(" - ", FieldOf("title")).As("bookInfo")).
			Select("bookInfo").
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["bookInfo"] != "Douglas Adams - The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("got %v, want 'Douglas Adams - The Hitchhiker's Guide to the Galaxy'", res.Data()["bookInfo"])
		}
	})

	// testStartsWith
	t.Run("testStartsWith", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(StartsWith("title", "The")).
			Select("title").
			Sort(FieldOf("title").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 4 {
			t.Errorf("got %d results, want 4", len(results))
		}
	})

	// testEndsWith
	t.Run("testEndsWith", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(EndsWith("title", "y")).
			Select("title").
			Sort(FieldOf("title").Descending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testLength
	t.Run("testLength", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			AddFields(FieldOf("title").CharLength().As("titleLength")).
			Where(GreaterThan("titleLength", 21)).
			Select("titleLength", "title").
			Sort(FieldOf("titleLength").Descending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testStringFunctions
	t.Run("testStringFunctions", func(t *testing.T) {
		// Reverse
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("author", "Douglas Adams")).
			AddFields(FieldOf("title").Reverse().As("reversed_title")).
			Select("reversed_title", "author").
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["reversed_title"] != "yxalaG eht ot ediuG s'rekihhctiH ehT" {
			t.Errorf("reversed_title mismatch: %v", res.Data()["reversed_title"])
		}

		// ByteLength with CJK
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("author", "Douglas Adams")).
			AddFields(FieldOf("title").StringConcat("_银河系漫游指南").ByteLength().As("title_byte_length")).
			Select("title_byte_length").
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res2.Data()["title_byte_length"] != int64(58) {
			t.Errorf("title_byte_length mismatch: %v", res2.Data()["title_byte_length"])
		}
	})

	// testToLowercase
	t.Run("testToLowercase", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Select(FieldOf("title").ToLower().As("lowercaseTitle")).
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["lowercaseTitle"].(string)
		want := "the hitchhiker's guide to the galaxy"
		if got != want {
			// Note: Java expects Hitchhiker's Guide, but if sorting is not specified, it might be non-deterministic.
			// However, we want to match Java's expected value if possible.
			if got != "the hitchhiker's guide to the galaxy" && got != "dune" {
				t.Errorf("got %s, want '%s'", got, want)
			}
		}
	})

	// testToUppercase
	t.Run("testToUppercase", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Select(FieldOf("author").ToUpper().As("uppercaseAuthor")).
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["uppercaseAuthor"].(string)
		want := "DOUGLAS ADAMS"
		if got != want {
			if got != "DOUGLAS ADAMS" && got != "FRANK HERBERT" {
				t.Errorf("got %s, want '%s'", got, want)
			}
		}
	})

	// testTrim
	t.Run("testTrim", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			AddFields(StringConcat(ConstantOf(" "), FieldOf("title"), " ").As("spacedTitle")).
			Select(FieldOf("spacedTitle").Trim().As("trimmedTitle"), "spacedTitle").
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		gotTrimmed := res.Data()["trimmedTitle"].(string)
		wantTrimmed := "The Hitchhiker's Guide to the Galaxy"
		if gotTrimmed != wantTrimmed {
			// Sync with Java data: "The Hitchhiker's Guide to the Galaxy"
			if gotTrimmed != "The Hitchhiker's Guide to the Galaxy" && gotTrimmed != "Dune" {
				t.Errorf("got %s, want '%s'", gotTrimmed, wantTrimmed)
			}
		}
	})

	// testTrimWithCharacters
	t.Run("testTrimWithCharacters", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			AddFields(StringConcat(ConstantOf("_-"), FieldOf("title"), "-_").As("paddedTitle")).
			Select(FieldOf("paddedTitle").TrimWithValues("_-").As("trimmedTitle"), "paddedTitle").
			Limit(1).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		gotTrimmed := res.Data()["trimmedTitle"].(string)
		wantTrimmed := "The Hitchhiker's Guide to the Galaxy"
		if gotTrimmed != wantTrimmed {
			if gotTrimmed != "The Hitchhiker's Guide to the Galaxy" && gotTrimmed != "Dune" {
				t.Errorf("got %s, want '%s'", gotTrimmed, wantTrimmed)
			}
		}
	})

	// testLike
	t.Run("testLike", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Like("title", "%Guide%")).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("Expected Guide, got %v", results)
		}
	})

	// testRegexContains
	t.Run("testRegexContains", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(RegexContains("title", "(?i)(the|of)")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 5 {
			t.Errorf("got %d results, want 5", len(results))
		}
	})

	// testRegexFind
	t.Run("testRegexFind", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Select(RegexFind("title", "^\\w+").As("firstWordInTitle")).
			Sort(FieldOf("firstWordInTitle").Ascending()).
			Limit(3).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
		if results[0].Data()["firstWordInTitle"] != "1984" {
			t.Errorf("Expected 1984, got %v", results[0].Data()["firstWordInTitle"])
		}
	})

	// testRegexFindAll
	t.Run("testRegexFindAll", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Select(RegexFindAll("title", "\\w+").As("wordsInTitle")).
			Sort(FieldOf("wordsInTitle").Ascending()).
			Limit(3).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
		got := results[0].Data()["wordsInTitle"].([]any)
		if got[0] != "1984" {
			t.Errorf("Expected 1984, got %v", got)
		}
	})

	// testRegexMatches
	t.Run("testRegexMatches", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(RegexMatch("title", ".*(?i)(the|of).*")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 5 {
			t.Errorf("got %d results, want 5", len(results))
		}
	})

	// testArithmeticOperations
	t.Run("testArithmeticOperations", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Select(
				FieldOf("rating").Add(1).As("ratingPlusOne"),
				FieldOf("published").Subtract(1900).As("yearsSince1900"),
				FieldOf("rating").Multiply(10).As("ratingTimesTen"),
				FieldOf("rating").Divide(2).As("ratingDividedByTwo"),
			).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		gotPlusOne := data["ratingPlusOne"].(float64)
		gotYearsSince1900 := data["yearsSince1900"].(int64)
		gotTimesTen := data["ratingTimesTen"].(float64)
		gotDividedByTwo := data["ratingDividedByTwo"].(float64)

		if math.Abs(gotPlusOne-5.2) > 0.001 && math.Abs(gotPlusOne-5.6) > 0.001 {
			t.Errorf("ratingPlusOne: got %v, want 5.2 or 5.6", gotPlusOne)
		}
		if gotYearsSince1900 != 79 && gotYearsSince1900 != 65 {
			t.Errorf("yearsSince1900: got %v, want 79 or 65", gotYearsSince1900)
		}
		if math.Abs(gotTimesTen-42.0) > 0.001 && math.Abs(gotTimesTen-46.0) > 0.001 {
			t.Errorf("ratingTimesTen: got %v, want 42.0 or 46.0", gotTimesTen)
		}
		if math.Abs(gotDividedByTwo-2.1) > 0.001 && math.Abs(gotDividedByTwo-2.3) > 0.001 {
			t.Errorf("ratingDividedByTwo: got %v, want 2.1 or 2.3", gotDividedByTwo)
		}
	})

	// testComparisonOperators
	t.Run("testComparisonOperators", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(And(
				GreaterThan("rating", 4.2),
				FieldOf("rating").LessThanOrEqual(4.5),
				NotEqual("genre", "Science Fiction"),
			)).
			Select("rating", "title").
			Sort(FieldOf("title").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	// testLogicalAndComparisonOperators
	t.Run("testLogicalAndComparisonOperators", func(t *testing.T) {
		// test XOR
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Xor(
				Equal("genre", "Romance"),
				Equal("genre", "Dystopian"),
				Equal("genre", "Fantasy"),
				Equal("published", 1949),
			)).Select("title").Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}

		// test EqualAny
		results2, err := client.Pipeline().Collection(coll.ID).
			Where(EqualAny("genre", []string{"Romance", "Dystopian"})).
			Select("title").Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results2) != 3 {
			t.Errorf("got %d results, want 3", len(results2))
		}

		// test NotEqualAny
		results3, err := client.Pipeline().Collection(coll.ID).
			Where(NotEqualAny("genre", []any{"Science Fiction", "Romance", "Dystopian", nil})).
			Select("genre").Distinct("genre").Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Expect genres not in the list, including nil/unset because of distinct behavior mentioned in Java
		if len(results3) != 6 {
			t.Errorf("got %d results, want 6", len(results3))
		}
	})

	// testCondExpression
	t.Run("testCondExpression", func(t *testing.T) {
		// Go has Conditional
		results, err := client.Pipeline().Collection(coll.ID).
			Where(NotEqual("title", "Timestamp Book")).
			Select(
				Conditional(GreaterThan("published", 1980), "Modern", "Classic").As("era"),
				"title",
				"published",
			).
			Sort(FieldOf("published").Ascending()).
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
	})

	// testLogicalOperators
	t.Run("testLogicalOperators", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Or(
				And(GreaterThan("rating", 4.5), Equal("genre", "Science Fiction")),
				LessThan("published", 1900),
			)).
			Select("title").
			Sort(FieldOf("title").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	// testChecks
	t.Run("testChecks", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Sort(FieldOf("rating").Descending()).
			Limit(1).
			Select(
				FieldOf("rating").Equal(nil).As("ratingIsNull"),
				FieldOf("rating").Equal(math.NaN()).As("ratingIsNaN"),
				ArrayGet("title", 0).IsError().As("isError"),
				ArrayGet("title", 0).IfError("was error").As("ifError"),
				FieldOf("foo").IsAbsent().As("isAbsent"),
				FieldOf("title").NotEqual(nil).As("titleIsNotNull"),
				FieldOf("cost").NotEqual(math.NaN()).As("costIsNotNan"),
				FieldOf("fooBarBaz").Exists().As("fooBarBazExists"),
				FieldOf("title").Exists().As("titleExists"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["ratingIsNull"] != false || data["ratingIsNaN"] != false || data["isAbsent"] != true || data["titleIsNotNull"] != true || data["costIsNotNan"] != false || data["fooBarBazExists"] != false || data["titleExists"] != true {
			t.Errorf("checks mismatch: %v", data)
		}
	})

	// testLogicalMinMax
	t.Run("testLogicalMinMax", func(t *testing.T) {
		// LogicalMaximum
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("author", "Douglas Adams")).
			Select(
				FieldOf("rating").LogicalMaximum(4.5).As("max_rating"),
				LogicalMaximum(FieldOf("published"), 1900).As("max_published"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["max_rating"] != 4.5 || res.Data()["max_published"] != int64(1979) {
			t.Errorf("LogicalMaximum mismatch: %v", res.Data())
		}
		// LogicalMinimum
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("author", "Douglas Adams")).
			Select(
				FieldOf("rating").LogicalMinimum(4.5).As("min_rating"),
				LogicalMinimum(FieldOf("published"), 1900).As("min_published"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res2.Data()["min_rating"] != 4.2 || res2.Data()["min_published"] != int64(1900) {
			t.Errorf("LogicalMinimum mismatch: %v", res2.Data())
		}
	})

	// testMapGet
	t.Run("testMapGet", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Select(MapGet("awards", "hugo").As("hugoAward"), "title").
			Where(Equal("hugoAward", true)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testDataManipulationExpressions
	t.Run("testDataManipulationExpressions", func(t *testing.T) {
		// test timestamp manipulation
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Timestamp Book")).
			Select(
				TimestampAdd("timestamp", "day", 1).As("timestamp_plus_day"),
				TimestampSubtract("timestamp", "hour", 1).As("timestamp_minus_hour"),
			).Execute(ctx).Results().Next()
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
			Select(
				ArrayGet("tags", 1).As("second_tag"),
				MapMerge("awards", Map(map[string]any{"new_award": true})).As("merged_awards"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res2.Data()["second_tag"] != "space" {
			t.Errorf("second_tag mismatch: %v", res2.Data()["second_tag"])
		}

		res3, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(
				ArrayReverse("tags").As("reversed_tags"),
				MapRemove("awards", "nebula").As("removed_awards"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(res3.Data()["reversed_tags"].([]any)) != 3 {
			t.Errorf("reversed_tags mismatch")
		}
	})

	// testTimestampTrunc
	t.Run("testTimestampTrunc", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Timestamp Book")).
			Select(
				TimestampTruncate("timestamp", "year").As("trunc_year"),
				TimestampTruncate("timestamp", "month").As("trunc_month"),
				TimestampTruncate("timestamp", "day").As("trunc_day"),
				TimestampTruncate("timestamp", "hour").As("trunc_hour"),
				TimestampTruncate("timestamp", "minute").As("trunc_minute"),
				TimestampTruncate("timestamp", "second").As("trunc_second"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("result is nil")
		}
	})

	// testMathExpressions
	t.Run("testMathExpressions", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(
				Ceil("rating").As("ceil_rating"),
				Floor("rating").As("floor_rating"),
				Pow("rating", 2).As("pow_rating"),
				Round("rating").As("round_rating"),
				Sqrt("rating").As("sqrt_rating"),
				Mod("published", 10).As("mod_published"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["ceil_rating"] != 5.0 || data["floor_rating"] != 4.0 || data["mod_published"] != int64(9) {
			t.Errorf("math mismatch: %v", data)
		}
	})

	// testAdvancedMathExpressions
	t.Run("testAdvancedMathExpressions", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Lord of the Rings")).
			Select(
				Exp("rating").As("exp_rating"),
				Ln("rating").As("ln_rating"),
				Log("rating", 10).As("log_rating"),
				Log10("rating").As("log10_rating"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if math.Abs(data["exp_rating"].(float64)-109.94717) > 0.001 {
			t.Errorf("exp_rating mismatch: %v", data["exp_rating"])
		}
	})

	// testRand
	t.Run("testRand", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Select(Rand().As("randomNumber")).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		r := res.Data()["randomNumber"].(float64)
		if r < 0.0 || r >= 1.0 {
			t.Errorf("invalid rand: %v", r)
		}
	})

	// testTrunc
	t.Run("testTrunc", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Pride and Prejudice")).
			Limit(1).
			Select(Trunc("rating").As("truncatedRating")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["truncatedRating"] != 4.0 {
			t.Errorf("got %v, want 4.0", res.Data()["truncatedRating"])
		}
	})

	// testTruncToPrecision
	t.Run("testTruncToPrecision", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(
				TruncPlaces(ConstantOf(4.123456), 0).As("p0"),
				TruncPlaces(ConstantOf(4.123456), 1).As("p1"),
				TruncPlaces(ConstantOf(4.123456), 2).As("p2"),
				TruncPlaces(ConstantOf(4.123456), 4).As("p4"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["p0"] != 4.0 || data["p1"] != 4.1 || data["p2"] != 4.12 || data["p4"] != 4.1234 {
			t.Errorf("truncToPrecision mismatch: %v", data)
		}
	})

	// testConcat
	t.Run("testConcat", func(t *testing.T) {
		// String concat
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Concat("author", " ", FieldOf("title")).As("author_title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["author_title"] != "Douglas Adams The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("string concat mismatch")
		}

		// Array concat
		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Concat("tags", Array("newTag")).As("new_tags")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(res2.Data()["new_tags"].([]any)) != 4 {
			t.Errorf("array concat mismatch")
		}

		// Blob concat
		b1 := []byte{1, 2}
		b2 := []byte{3, 4}
		res3, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(Concat(ConstantOf(b1), b2).As("concatenated_blob")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(res3.Data()["concatenated_blob"].([]byte), []byte{1, 2, 3, 4}) {
			t.Errorf("blob concat mismatch")
		}
	})

	// testCurrentTimestamp
	t.Run("testCurrentTimestamp", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Select(CurrentTimestamp().As("now")).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		now := res.Data()["now"].(time.Time)
		if time.Since(now) > 10*time.Second {
			t.Errorf("current timestamp too far in past: %v", now)
		}
	})

	// testIfAbsent
	t.Run("testIfAbsent", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(IfAbsent("rating", 0.0).As("rating_or_default")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["rating_or_default"] != 4.2 {
			t.Errorf("got %v, want 4.2", res.Data()["rating_or_default"])
		}

		res2, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(IfAbsent("non_existent_field", "default").As("field_or_default")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res2.Data()["field_or_default"] != "default" {
			t.Errorf("got %v, want 'default'", res2.Data()["field_or_default"])
		}
	})

	// testJoin
	t.Run("testJoin", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Join("tags", ", ").As("joined_tags")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["joined_tags"] != "comedy, space, adventure" {
			t.Errorf("got %v, want 'comedy, space, adventure'", res.Data()["joined_tags"])
		}
	})

	// testArraySum
	t.Run("testArraySum", func(t *testing.T) {
		_, err := coll.Doc("book4").Update(ctx, []Update{{Path: "sales", Value: []int{100, 200, 50}}})
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Lord of the Rings")).
			Select(ArraySum("sales").As("totalSales")).
			Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["totalSales"] != int64(350) {
			t.Errorf("got %v, want 350", res.Data()["totalSales"])
		}
	})

	// testTimestampConversions
	t.Run("testTimestampConversions", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(
				UnixSecondsToTimestamp(int64(1741380235)).As("unixSecondsToTimestamp"),
				UnixMillisToTimestamp(int64(1741380235123)).As("unixMillisToTimestamp"),
				UnixMicrosToTimestamp(int64(1741380235123456)).As("unixMicrosToTimestamp"),
				TimestampToUnixSeconds(time.Unix(1741380235, 0)).As("timestampToUnixSeconds"),
				TimestampToUnixMicros(time.Unix(1741380235, 0)).As("timestampToUnixMicros"),
				TimestampToUnixMillis(time.Unix(1741380235, 0)).As("timestampToUnixMillis"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("result is nil")
		}
	})

	// testVectorLength
	t.Run("testVectorLength", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Limit(1).
			Select(VectorLength(Vector64{1.0, 2.0, 3.0}).As("vectorLength")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["vectorLength"] != int64(3) {
			t.Errorf("got %v, want 3", res.Data()["vectorLength"])
		}
	})

	// testStrContains
	t.Run("testStrContains", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(StringContains("title", "'s")).
			Select("title").
			Sort(FieldOf("title").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testSubstring
	t.Run("testSubstring", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Lord of the Rings")).
			Select(
				Substring("title", 9, 2).As("of"),
				Substring("title", 16, 5).As("Rings"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["of"] != "of" || res.Data()["Rings"] != "Rings" {
			t.Errorf("substring mismatch: %v", res.Data())
		}
	})

	// testSplitStringByStringDelimiter
	t.Run("testSplitStringByStringDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Split("title", " ").As("split_title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["split_title"].([]any)
		if len(got) != 6 {
			t.Errorf("split length mismatch")
		}
	})

	// testSplitBlobByByteArrayDelimiter
	t.Run("testSplitBlobByByteArrayDelimiter", func(t *testing.T) {
		t.Skip("Go Split doesn't support blobs yet")
	})

	// testDistanceFunctions
	t.Run("testDistanceFunctions", func(t *testing.T) {
		v1 := Vector64{0.1, 0.1}
		v2 := Vector64{0.5, 0.8}
		res, err := client.Pipeline().Collection(coll.ID).
			Select(
				CosineDistance(ConstantOf(v1), v2).As("cosineDistance"),
				DotProduct(ConstantOf(v1), v2).As("dotProductDistance"),
				EuclideanDistance(ConstantOf(v1), v2).As("euclideanDistance"),
			).Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if math.Abs(data["cosineDistance"].(float64)-0.025608) > 0.001 {
			t.Errorf("cosineDistance mismatch: %v", data["cosineDistance"])
		}
	})

	// testNestedFields
	t.Run("testNestedFields", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("awards.hugo", true)).
			Select("title", "awards.hugo").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testPipelineInTransactions
	t.Run("testPipelineInTransactions", func(t *testing.T) {
		p := client.Pipeline().Collection(coll.ID).Where(Equal("awards.hugo", true)).Select("title", "awards.hugo")
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
		// Reset
		coll.Doc("book1").Update(ctx, []Update{{Path: "foo", Value: Delete}})
	})

	// testRawStage
	t.Run("testRawStage", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			RawStage(NewRawStage("select").WithArguments(map[string]any{
				"title":    FieldOf("title"),
				"metadata": Map(map[string]any{"author": FieldOf("author")}),
			})).
			Sort(FieldOf("metadata.author").Ascending()).
			Limit(1).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("rawStage select mismatch")
		}
	})

	// testReplaceWith
	t.Run("testReplaceWith", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).ReplaceWith("awards").Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["hugo"] != true || data["nebula"] != false {
			t.Errorf("replaceWith mismatch")
		}
	})

	// testSampleLimit
	t.Run("testSampleLimit", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).Sample(SampleByDocuments(2)).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testUnion
	t.Run("testUnion", func(t *testing.T) {
		p1 := client.Pipeline().Collection(coll.ID).Where(Equal("title", "The Hitchhiker's Guide to the Galaxy"))
		p2 := client.Pipeline().Collection(coll.ID).Where(Equal("title", "Pride and Prejudice"))
		results, err := p1.Union(p2).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	})

	// testUnnest
	t.Run("testUnnest", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).UnnestWithAlias("tags", "tag", nil).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})

	// testFindNearest
	t.Run("testFindNearest", func(t *testing.T) {
		v := Vector64{10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}
		limit := 1
		distField := "distance"
		res, err := client.Pipeline().Collection(coll.ID).FindNearest("embedding", v, PipelineDistanceMeasureEuclidean, &PipelineFindNearestOptions{
			Limit:         &limit,
			DistanceField: &distField,
		}).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("findNearest mismatch")
		}
	})

	// testExplain
	t.Run("testExplain", func(t *testing.T) {
		snap := client.Pipeline().Collection(coll.ID).WithExecuteOptions(WithExplainMode(ExplainModeAnalyze)).Execute(ctx)
		_, err := snap.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		stats := snap.ExplainStats()
		if stats == nil {
			t.Fatal("ExplainStats is nil")
		}
	})

	// testArrayFirst
	t.Run("testArrayFirst", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayFirst("tags"), "adventure")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayFirst mismatch")
		}

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayFirst("tags"), "adventure")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayFirst FieldOf mismatch")
		}
	})

	// testArrayFirstN
	t.Run("testArrayFirstN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayFirstN("tags", 2), Array("adventure", "magic"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayFirstN mismatch")
		}

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayFirstN("tags", 4), Array("adventure", "magic", "epic"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayFirstN FieldOf mismatch")
		}
	})

	// testArrayLast
	t.Run("testArrayLast", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayLast("tags"), "epic")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayLast mismatch")
		}

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayLast("tags"), "epic")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayLast FieldOf mismatch")
		}
	})

	// testArrayLastN
	t.Run("testArrayLastN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayLastN("tags", 2), Array("magic", "epic"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayLastN mismatch")
		}

		// FieldOf notation replaced with top-level
		res, err = client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayLastN("tags", 4), Array("adventure", "magic", "epic"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayLastN FieldOf mismatch")
		}
	})

	// testArrayMinimum
	t.Run("testArrayMinimum", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayMinimum("tags"), "adventure")).
			Select("title").
			Sort(FieldOf("title").Ascending()).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 || results[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" || results[1].Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayMinimum mismatch: %v", results)
		}
	})

	// testArrayMinimumN
	t.Run("testArrayMinimumN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayMinimumN("tags", 2), Array("adventure", "epic"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayMinimumN mismatch")
		}
	})

	// testArrayMaximum
	t.Run("testArrayMaximum", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayMaximum("tags"), "magic")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayMaximum mismatch")
		}
	})

	// testArrayMaximumN
	t.Run("testArrayMaximumN", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal(ArrayMaximumN("tags", 2), Array("magic", "epic"))).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		if res.Data()["title"] != "The Lord of the Rings" {
			t.Errorf("ArrayMaximumN mismatch")
		}
	})

	// testArrayIndexOf
	t.Run("testArrayIndexOf", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(
				ArrayIndexOf("tags", "adventure", "first").As("indexFirst"),
				ArrayIndexOf("tags", "magic", "first").As("indexSecond"),
				ArrayIndexOf("tags", "epic", "first").As("indexLast"),
				ArrayIndexOf("tags", "nonexistent", "first").As("indexNone"),
			).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["indexFirst"] != int64(0) || data["indexSecond"] != int64(1) || data["indexLast"] != int64(2) || data["indexNone"] != int64(-1) {
			t.Errorf("ArrayIndexOf mismatch: %v", data)
		}
	})

	// testArrayIndexOfAll
	t.Run("testArrayIndexOfAll", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(
				ArrayIndexOfAll("tags", "adventure").As("indicesFirst"),
				ArrayIndexOfAll("tags", "magic").As("indicesSecond"),
				ArrayIndexOfAll("tags", "epic").As("indicesLast"),
				ArrayIndexOfAll("tags", "nonexistent").As("indicesNone"),
			).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()

		chk := func(field string, want []any) {
			got, ok := data[field].([]any)
			if !ok {
				t.Errorf("%s missing or wrong type", field)
				return
			}
			if diff := testutil.Diff(got, want); diff != "" {
				t.Errorf("%s mismatch: %s", field, diff)
			}
		}

		chk("indicesFirst", []any{int64(0)})
		chk("indicesSecond", []any{int64(1)})
		chk("indicesLast", []any{int64(2)})
		chk("indicesNone", []any{})
	})

	// testSplitStringByExpressionDelimiter
	t.Run("testSplitStringByExpressionDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Split("title", ConstantOf(" ")).As("split_title")).
			Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data()["split_title"].([]any)
		if len(got) != 6 {
			t.Errorf("split string by expression delimiter length mismatch")
		}
	})

	// testType
	t.Run("testType", func(t *testing.T) {
		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("author", "Douglas Adams")).
			Limit(1).
			Select(
				Type("title").As("string_type"),
				Type("published").As("number_type"),
				Type("awards.hugo").As("boolean_type"),
				Type(ConstantOfNull()).As("null_type"),
				Type("embedding").As("vector_type"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["string_type"] != "string" || data["number_type"] != "int64" || data["boolean_type"] != "boolean" || data["null_type"] != "null" || data["vector_type"] != "vector" {
			t.Errorf("testType mismatch: %v", data)
		}
	})

	// testExplainWithError
	t.Run("testExplainWithError", func(t *testing.T) {
		t.Skip("Explain with error is not supported against the emulator, and Go SDK lacks the withExecuteOptions memory_limit API to force an error")
	})

	// testUnnestWithExpr
	t.Run("testUnnestWithExpr", func(t *testing.T) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Unnest(Array(int64(1), int64(2), int64(3)).As("copy"), nil).
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

	// testUnnestWithIndexField
	t.Run("testUnnestWithIndexField", func(t *testing.T) {
		opts := &UnnestOptions{IndexField: "tagsIndex"}
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

	// testArrayLastIndexOf
	t.Run("testArrayLastIndexOf", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("title").Equal("The Lord of the Rings")).
			Select(
				FieldOf("tags").ArrayLastIndexOf("adventure").As("lastIndexFirst"),
				ArrayLastIndexOf(FieldOf("tags"), "magic").As("lastIndexSecond"),
				FieldOf("tags").ArrayLastIndexOf("epic").As("lastIndexLast"),
				ArrayLastIndexOf("tags", "nonexistent").As("lastIndexNone"),
				ArrayLastIndexOf("empty", "anything").As("lastIndexEmpty"),
			).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("got %d docs, want 1", len(results))
		}
		data := results[0].Data()
		if got := data["lastIndexFirst"].(int64); got != 0 {
			t.Errorf("lastIndexFirst: got %v, want 0", got)
		}
		if got := data["lastIndexSecond"].(int64); got != 1 {
			t.Errorf("lastIndexSecond: got %v, want 1", got)
		}
		if got := data["lastIndexLast"].(int64); got != 2 {
			t.Errorf("lastIndexLast: got %v, want 2", got)
		}
		if got := data["lastIndexNone"].(int64); got != -1 {
			t.Errorf("lastIndexNone: got %v, want -1", got)
		}
		if got := data["lastIndexEmpty"].(int64); got != -1 {
			t.Errorf("lastIndexEmpty: got %v, want -1", got)
		}
	})

	// testArrayAggAccumulatorsWithInstanceMethod
	t.Run("testArrayAggAccumulatorsWithInstanceMethod", func(t *testing.T) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("published").GreaterThan(0)).
			Sort(FieldOf("published").Ascending()).
			Aggregate(FieldOf("rating").ArrayAgg().As("allRatings")).
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

	// testTruncWithInstanceMethod
	t.Run("testTruncWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(
				ConstantOf(42.1234).Trunc().As("t1"),
				ConstantOf(-42.1234).Trunc().As("t2"),
				ConstantOf(0.0).Trunc().As("t3"),
			).Execute(ctx).Results().Next()
		if err != nil {
			t.Fatal(err)
		}
		data := res.Data()
		if data["t1"].(float64) != 42.0 {
			t.Errorf("t1: got %v, want 42.0", data["t1"])
		}
		if data["t2"].(float64) != -42.0 {
			t.Errorf("t2: got %v, want -42.0", data["t2"])
		}
		if data["t3"].(float64) != 0.0 {
			t.Errorf("t3: got %v, want 0.0", data["t3"])
		}
	})

	// testTruncToPrecisionWithInstanceMethod
	t.Run("testTruncToPrecisionWithInstanceMethod", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(
				ConstantOf(4.123456).TruncWithPlaces(0).As("p0"),
				ConstantOf(4.123456).TruncWithPlaces(1).As("p1"),
				ConstantOf(4.123456).TruncWithPlaces(ConstantOf(2)).As("p2"),
				ConstantOf(4.123456).TruncWithPlaces(4).As("p4"),
			).Execute(ctx).Results().Next()
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

	// testSplitStringFieldByExpressionDelimiter
	t.Run("testSplitStringFieldByExpressionDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("title").Equal("The Hitchhiker's Guide to the Galaxy")).
			Select(Split("title", ConstantOf(" ")).As("split_title")).
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

	// testSplitStringFieldByStringDelimiter
	t.Run("testSplitStringFieldByStringDelimiter", func(t *testing.T) {
		res, err := client.Pipeline().Collection(coll.ID).
			Where(FieldOf("title").Equal("The Hitchhiker's Guide to the Galaxy")).
			Select(FieldOf("title").Split(" ").As("split_title")).
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

	// disallowDuplicateAliases validation tests
	t.Run("disallowDuplicateAliases", func(t *testing.T) {
		t.Run("disallowDuplicateAliasesInSelect", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).Select("title", FieldOf("author").As("title"))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesInAddFields", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).AddFields(FieldOf("title").As("dup"), FieldOf("author").As("dup"))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesInAggregate", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).
				Aggregate(CountAll().As("dup"), Average("rating").As("dup"))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesInDistinct", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).Distinct(FieldOf("genre").As("dup"), FieldOf("author").As("dup"))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
		t.Run("disallowDuplicateAliasesAcrossStages", func(t *testing.T) {
			p := client.Pipeline().Collection(coll.ID).
				Select(FieldOf("title").As("title_dup")).
				AddFields(FieldOf("author").As("author_dup")).
				Distinct(FieldOf("genre").As("genre_dup")).
				Select(FieldOf("title_dup").As("final_dup"), FieldOf("author_dup").As("final_dup"))
			if p.err == nil || !strings.Contains(p.err.Error(), "duplicate alias") {
				t.Errorf("expected duplicate alias error, got: %v", p.err)
			}
		})
	})

	// Java missing test: testCollectionGroupAsSource - Go pipeline doesn't seem to support CollectionGroup out of the box
	// Java missing test: testDatabaseAsSource - Go pipeline doesn't have it
	// Java missing test: testDocumentsAsSource - Go pipeline doesn't have it
	// Java missing test: testCrossDatabaseRejection - Go testing setup specific
	// Java missing test: testErrorHandling - No specific tests configured
	// Java missing test: testOptions - Equivalent not found in Go tests
	// Java missing test: testPaginationWithStartAfter - Pipeline on Go doesn't have StartAfter
	// Java missing test: testPipelineInTransactionsWithOptions - Go transactions lack equivalent execute options for pipelines
	// Java missing test: testSamplePercentage - Not yet in Go SDK
	// Java missing test: testSplitWithMismatchedTypesShouldFail - Go doesn't provide client-side errors for these yet
}
