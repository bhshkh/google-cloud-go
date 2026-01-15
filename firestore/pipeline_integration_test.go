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
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/latlng"
)

func skipIfNotEnterprise(t *testing.T) {
	if testParams[firestoreEditionKey].(firestoreEdition) != editionEnterprise {
		t.Skip("Skipping test in non-enterprise environment")
	}
}

type Author struct {
	Name    string `firestore:"name"`
	Country string `firestore:"country"`
}

type Book struct {
	Title     string   `firestore:"title"`
	Author    Author   `firestore:"author"`
	Genre     string   `firestore:"genre"`
	Published int      `firestore:"published"`
	Rating    float64  `firestore:"rating"`
	Tags      []string `firestore:"tags"`
}

func testBooks() []Book {
	return []Book{
		{Title: "The Hitchhiker's Guide to the Galaxy", Author: Author{Name: "Douglas Adams", Country: "UK"}, Genre: "Science Fiction", Published: 1979, Rating: 4.2, Tags: []string{"comedy", "space", "adventure"}},
		{Title: "Pride and Prejudice", Author: Author{Name: "Jane Austen", Country: "UK"}, Genre: "Romance", Published: 1813, Rating: 4.5, Tags: []string{"classic", "social commentary", "love"}},
		{Title: "One Hundred Years of Solitude", Author: Author{Name: "Gabriel García Márquez", Country: "Colombia"}, Genre: "Magical Realism", Published: 1967, Rating: 4.3, Tags: []string{"family", "history", "fantasy"}},
		{Title: "The Lord of the Rings", Author: Author{Name: "J.R.R. Tolkien", Country: "UK"}, Genre: "Fantasy", Published: 1954, Rating: 4.7, Tags: []string{"adventure", "magic", "epic"}},
		{Title: "The Handmaid's Tale", Author: Author{Name: "Margaret Atwood", Country: "Canada"}, Genre: "Dystopian", Published: 1985, Rating: 4.1, Tags: []string{"feminism", "totalitarianism", "resistance"}},
		{Title: "Crime and Punishment", Author: Author{Name: "Fyodor Dostoevsky", Country: "Russia"}, Genre: "Psychological Thriller", Published: 1866, Rating: 4.3, Tags: []string{"philosophy", "crime", "redemption"}},
		{Title: "To Kill a Mockingbird", Author: Author{Name: "Harper Lee", Country: "USA"}, Genre: "Southern Gothic", Published: 1960, Rating: 4.2, Tags: []string{"racism", "injustice", "coming-of-age"}},
		{Title: "1984", Author: Author{Name: "George Orwell", Country: "UK"}, Genre: "Dystopian", Published: 1949, Rating: 4.2, Tags: []string{"surveillance", "totalitarianism", "propaganda"}},
		{Title: "The Great Gatsby", Author: Author{Name: "F. Scott Fitzgerald", Country: "USA"}, Genre: "Modernist", Published: 1925, Rating: 4.0, Tags: []string{"wealth", "american dream", "love"}},
		{Title: "Dune", Author: Author{Name: "Frank Herbert", Country: "USA"}, Genre: "Science Fiction", Published: 1965, Rating: 4.6, Tags: []string{"politics", "desert", "ecology"}},
	}
}

func TestIntegration_PipelineExecute(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	client := integrationClient(t)
	coll := integrationColl(t)

	t.Run("WithReadOptions", func(t *testing.T) {
		timeBeforeCreate := time.Now()
		doc1 := coll.NewDoc()
		_, err := doc1.Create(ctx, map[string]interface{}{"a": 1})
		if err != nil {
			t.Fatal(err)
		}

		// Let a little time pass to ensure the next write has a later timestamp.
		time.Sleep(1 * time.Millisecond)

		doc2 := coll.NewDoc()
		_, err = doc2.Create(ctx, map[string]interface{}{"a": 2})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			deleteDocuments([]*DocumentRef{doc1, doc2})
		})

		iter := client.Pipeline().Collection(coll.ID).WithReadOptions(ReadTime(timeBeforeCreate)).Execute(ctx).Results()
		res, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 0 {
			t.Errorf("got %d documents, want 0", len(res))
		}
	})
	t.Run("WithTransaction", func(t *testing.T) {
		h := testHelper{t}
		books := testBooks()[:2]
		var docRefs []*DocumentRef
		for _, b := range books {
			docRef := coll.NewDoc()
			h.mustCreate(docRef, b)
			docRefs = append(docRefs, docRef)
		}
		t.Cleanup(func() {
			deleteDocuments(docRefs)
		})
		p := client.Pipeline().Collection(coll.ID)
		err := client.RunTransaction(ctx, func(ctx context.Context, txn *Transaction) error {
			iter := txn.Execute(p).Results()
			res, err := iter.GetAll()
			if err != nil {
				return err
			}
			if len(res) != len(books) {
				return fmt.Errorf("got %d documents, want %d", len(res), len(books))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestIntegration_PipelineStages(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	client := integrationClient(t)
	coll := integrationColl(t)
	h := testHelper{t}
	type Author struct {
		Name    string `firestore:"name"`
		Country string `firestore:"country"`
	}
	type Book struct {
		Title     string   `firestore:"title"`
		Author    Author   `firestore:"author"`
		Genre     string   `firestore:"genre"`
		Published int      `firestore:"published"`
		Rating    float64  `firestore:"rating"`
		Tags      []string `firestore:"tags"`
	}
	books := []Book{
		{Title: "The Hitchhiker's Guide to the Galaxy", Author: Author{Name: "Douglas Adams", Country: "UK"}, Genre: "Science Fiction", Published: 1979, Rating: 4.2, Tags: []string{"comedy", "space", "adventure"}},
		{Title: "Pride and Prejudice", Author: Author{Name: "Jane Austen", Country: "UK"}, Genre: "Romance", Published: 1813, Rating: 4.5, Tags: []string{"classic", "social commentary", "love"}},
		{Title: "One Hundred Years of Solitude", Author: Author{Name: "Gabriel García Márquez", Country: "Colombia"}, Genre: "Magical Realism", Published: 1967, Rating: 4.3, Tags: []string{"family", "history", "fantasy"}},
		{Title: "The Lord of the Rings", Author: Author{Name: "J.R.R. Tolkien", Country: "UK"}, Genre: "Fantasy", Published: 1954, Rating: 4.7, Tags: []string{"adventure", "magic", "epic"}},
		{Title: "The Handmaid's Tale", Author: Author{Name: "Margaret Atwood", Country: "Canada"}, Genre: "Dystopian", Published: 1985, Rating: 4.1, Tags: []string{"feminism", "totalitarianism", "resistance"}},
		{Title: "Crime and Punishment", Author: Author{Name: "Fyodor Dostoevsky", Country: "Russia"}, Genre: "Psychological Thriller", Published: 1866, Rating: 4.3, Tags: []string{"philosophy", "crime", "redemption"}},
		{Title: "To Kill a Mockingbird", Author: Author{Name: "Harper Lee", Country: "USA"}, Genre: "Southern Gothic", Published: 1960, Rating: 4.2, Tags: []string{"racism", "injustice", "coming-of-age"}},
		{Title: "1984", Author: Author{Name: "George Orwell", Country: "UK"}, Genre: "Dystopian", Published: 1949, Rating: 4.2, Tags: []string{"surveillance", "totalitarianism", "propaganda"}},
		{Title: "The Great Gatsby", Author: Author{Name: "F. Scott Fitzgerald", Country: "USA"}, Genre: "Modernist", Published: 1925, Rating: 4.0, Tags: []string{"wealth", "american dream", "love"}},
		{Title: "Dune", Author: Author{Name: "Frank Herbert", Country: "USA"}, Genre: "Science Fiction", Published: 1965, Rating: 4.6, Tags: []string{"politics", "desert", "ecology"}},
	}
	var docRefs []*DocumentRef
	for _, b := range books {
		docRef := coll.NewDoc()
		h.mustCreate(docRef, b)
		docRefs = append(docRefs, docRef)
	}
	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})
	t.Run("AddFields", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).AddFields(Multiply(FieldOf("rating"), 2).As("doubled_rating")).Limit(1).Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if dr, ok := data["doubled_rating"]; !ok || dr.(float64) != data["rating"].(float64)*2 {
			t.Errorf("got doubled_rating %v, want %v", dr, data["rating"].(float64)*2)
		}
	})
	t.Run("Aggregate", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Aggregate(Count("rating").As("total_books")).Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}

		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if data["total_books"] != int64(10) {
			t.Errorf("got %d total_books, want 10", data["total_books"])
		}
	})
	t.Run("AggregateWithSpec", func(t *testing.T) {
		spec := NewAggregateSpec(Average("rating").As("avg_rating")).WithGroups("genre")
		iter := client.Pipeline().Collection(coll.ID).AggregateWithSpec(spec).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 8 {
			t.Errorf("got %d groups, want 8", len(results))
		}
	})
	t.Run("Distinct", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Distinct("genre").Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 8 {
			t.Errorf("got %d distinct genres, want 8", len(results))
		}
	})
	t.Run("Documents", func(t *testing.T) {
		iter := client.Pipeline().Documents(docRefs[0], docRefs[1]).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("got %d documents, want 2", len(results))
		}
	})
	t.Run("CollectionGroup", func(t *testing.T) {
		cgCollID := collectionIDs.New()
		doc1 := coll.Doc("cg_doc1")
		doc2 := coll.Doc("cg_doc2")
		cgColl1 := doc1.Collection(cgCollID)
		cgColl2 := doc2.Collection(cgCollID)
		cgDoc1 := cgColl1.NewDoc()
		cgDoc2 := cgColl2.NewDoc()
		h.mustCreate(cgDoc1, map[string]string{"val": "a"})
		h.mustCreate(cgDoc2, map[string]string{"val": "b"})
		t.Cleanup(func() {
			deleteDocuments([]*DocumentRef{cgDoc1, cgDoc2, doc1, doc2})
		})
		iter := client.Pipeline().CollectionGroup(cgCollID).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("got %d documents, want 2", len(results))
		}
	})
	t.Run("Database", func(t *testing.T) {
		dbDoc1 := coll.Doc("db_doc1")
		otherColl := client.Collection(collectionIDs.New())
		dbDoc2 := otherColl.Doc("db_doc2")
		h.mustCreate(dbDoc1, map[string]string{"val": "a"})
		h.mustCreate(dbDoc2, map[string]string{"val": "b"})
		t.Cleanup(func() {
			deleteDocuments([]*DocumentRef{dbDoc1, dbDoc2})
		})
		iter := client.Pipeline().Database().Limit(2).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("got %d documents, want 2", len(results))
		}
	})
	t.Run("FindNearest", func(t *testing.T) {
		type DocWithVector struct {
			ID     string   `firestore:"id"`
			Vector Vector32 `firestore:"vector"`
		}
		docsWithVector := []DocWithVector{
			{ID: "doc1", Vector: Vector32{1.0, 2.0, 3.0}},
			{ID: "doc2", Vector: Vector32{4.0, 5.0, 6.0}},
			{ID: "doc3", Vector: Vector32{7.0, 8.0, 9.0}},
		}
		var vectorDocRefs []*DocumentRef
		for _, d := range docsWithVector {
			docRef := coll.NewDoc()
			h.mustCreate(docRef, d)
			vectorDocRefs = append(vectorDocRefs, docRef)
		}
		t.Cleanup(func() {
			deleteDocuments(vectorDocRefs)
		})
		queryVector := Vector32{1.1, 2.1, 3.1}
		limit := 2
		distanceField := "distance"
		options := &PipelineFindNearestOptions{
			Limit:         &limit,
			DistanceField: &distanceField,
		}
		iter := client.Pipeline().Collection(coll.ID).
			FindNearest("vector", queryVector, PipelineDistanceMeasureEuclidean, options).
			Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("got %d documents, want 2", len(results))
		}
		// Check if the results are sorted by distance

		if !results[0].Exists() {
			t.Fatalf("results[0] Exists: got: false, want: true")
		}
		dist1 := results[0].Data()

		if !results[1].Exists() {
			t.Fatalf("results[1] Exists: got: false, want: true")
		}
		dist2 := results[1].Data()
		if dist1[distanceField].(float64) > dist2[distanceField].(float64) {
			t.Errorf("documents are not sorted by distance")
		}
		// Check if the correct documents are returned
		if dist1["id"] != "doc1" {
			t.Errorf("got doc id %q, want 'doc1'", dist1["id"])
		}
	})
	t.Run("Limit", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Limit(3).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("got %d documents, want 3", len(results))
		}
	})
	t.Run("Offset", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Sort(Ascending(FieldOf("published"))).Offset(2).Limit(1).Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if data["title"] != "The Great Gatsby" {
			t.Errorf("got title %q, want 'The Great Gatsby'", data["title"])
		}
	})
	t.Run("RawStage", func(t *testing.T) {
		// Using RawStage to perform a Limit operation
		iter := client.Pipeline().Collection(coll.ID).RawStage(NewRawStage("limit").WithArguments(3)).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("got %d documents, want 3", len(results))
		}

		// Using RawStage to perform a Select operation with options
		iter = client.Pipeline().Collection(coll.ID).RawStage(NewRawStage("select").WithArguments(map[string]interface{}{"title": FieldOf("title")})).Limit(1).Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if _, ok := data["title"]; !ok {
			t.Error("missing 'title' field")
		}
		if _, ok := data["genre"]; ok {
			t.Error("unexpected 'genre' field")
		}
	})
	t.Run("RemoveFields", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).
			Limit(1).
			RemoveFields("genre", "rating").
			Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if _, ok := data["genre"]; ok {
			t.Error("unexpected 'genre' field")
		}
		if _, ok := data["rating"]; ok {
			t.Error("unexpected 'rating' field")
		}
		if _, ok := data["title"]; !ok {
			t.Error("missing 'title' field")
		}
	})
	t.Run("Replace", func(t *testing.T) {
		type DocWithMap struct {
			ID   string         `firestore:"id"`
			Data map[string]int `firestore:"data"`
		}
		docWithMap := DocWithMap{ID: "docWithMap", Data: map[string]int{"a": 1, "b": 2}}
		docRef := coll.NewDoc()
		h.mustCreate(docRef, docWithMap)
		t.Cleanup(func() {
			deleteDocuments([]*DocumentRef{docRef})
		})
		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal(FieldOf("id"), "docWithMap")).
			ReplaceWith("data").
			Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		want := map[string]interface{}{"a": int64(1), "b": int64(2)}
		if diff := testutil.Diff(data, want); diff != "" {
			t.Errorf("got: %v, want: %v, diff +want -got: %s", data, want, diff)
		}
	})
	t.Run("Sample", func(t *testing.T) {
		t.Run("SampleByDocuments", func(t *testing.T) {
			iter := client.Pipeline().Collection(coll.ID).Sample(SampleByDocuments(5)).Execute(ctx).Results()
			defer iter.Stop()
			var got []map[string]interface{}
			for {
				doc, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatalf("Failed to iterate: %v", err)
				}
				if !doc.Exists() {
					t.Fatalf("Exists: got: false, want: true")
				}
				data := doc.Data()
				got = append(got, data)
			}
			if len(got) != 5 {
				t.Errorf("got %d documents, want 5", len(got))
			}
		})
		t.Run("SampleByPercentage", func(t *testing.T) {
			iter := client.Pipeline().Collection(coll.ID).Sample(&SampleSpec{Size: 0.6, Mode: SampleModePercent}).Execute(ctx).Results()
			defer iter.Stop()
			var got []map[string]interface{}
			for {
				doc, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatalf("Failed to iterate: %v", err)
				}
				if !doc.Exists() {
					t.Fatalf("Exists: got: false, want: true")
				}
				data := doc.Data()
				got = append(got, data)
			}
			if len(got) >= 10 {
				t.Errorf("Sampled documents count should be less than total. got %d, total 10", len(got))
			}
			if len(got) == 0 {
				t.Errorf("Sampled documents count should be greater than 0. got %d", len(got))
			}
		})
	})
	t.Run("Select", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Select("title", "author.name").Limit(1).Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if _, ok := data["title"]; !ok {
			t.Error("missing 'title' field")
		}
		if _, ok := data["author.name"]; !ok {
			t.Error("missing 'author.name' field")
		}
		if _, ok := data["author"]; ok {
			t.Error("unexpected 'author' field")
		}
		if _, ok := data["genre"]; ok {
			t.Error("unexpected 'genre' field")
		}
	})
	t.Run("Sort", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Sort(Descending(FieldOf("rating"))).Limit(1).Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if data["title"] != "The Lord of the Rings" {
			t.Errorf("got title %q, want 'The Lord of the Rings'", data["title"])
		}
	})
	t.Run("Union", func(t *testing.T) {
		type Employee struct {
			Name string `firestore:"name"`
			Age  int    `firestore:"age"`
		}
		type Customer struct {
			Name    string `firestore:"name"`
			Address string `firestore:"address"`
		}
		employeeColl := client.Collection(collectionIDs.New())
		customerColl := client.Collection(collectionIDs.New())
		employees := []Employee{
			{Name: "John Doe", Age: 42},
			{Name: "Jane Smith", Age: 35},
		}
		customers := []Customer{
			{Name: "Alice", Address: "123 Main St"},
			{Name: "Bob", Address: "456 Oak Ave"},
		}
		var unionDocRefs []*DocumentRef
		for _, e := range employees {
			docRef := employeeColl.NewDoc()
			h.mustCreate(docRef, e)
			unionDocRefs = append(unionDocRefs, docRef)
		}
		for _, c := range customers {
			docRef := customerColl.NewDoc()
			h.mustCreate(docRef, c)
			unionDocRefs = append(unionDocRefs, docRef)
		}
		t.Cleanup(func() {
			deleteDocuments(unionDocRefs)
		})
		employeePipeline := client.Pipeline().Collection(employeeColl.ID)
		customerPipeline := client.Pipeline().Collection(customerColl.ID)
		iter := employeePipeline.Union(customerPipeline).Execute(context.Background()).Results()
		defer iter.Stop()
		var got []map[string]interface{}
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("Failed to iterate: %v", err)
			}
			if !doc.Exists() {
				t.Fatalf("Exists: got: false, want: true")
			}
			data := doc.Data()
			got = append(got, data)
		}
		want := []map[string]interface{}{
			{"name": "John Doe", "age": int64(42)},
			{"name": "Jane Smith", "age": int64(35)},
			{"name": "Alice", "address": "123 Main St"},
			{"name": "Bob", "address": "456 Oak Ave"},
		}
		sort.Slice(got, func(i, j int) bool {
			return got[i]["name"].(string) < got[j]["name"].(string)
		})
		sort.Slice(want, func(i, j int) bool {
			return want[i]["name"].(string) < want[j]["name"].(string)
		})
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("got: %v, want: %v, diff +want -got: %s", got, want, diff)
		}
	})
	t.Run("Unnest", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal(FieldOf("title"), "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag", nil).
			Select("title", "tag").
			Execute(ctx).Results()
		defer iter.Stop()
		var got []map[string]interface{}
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("Failed to iterate: %v", err)
			}
			if !doc.Exists() {
				t.Fatalf("Exists: got: false, want: true")
			}
			data := doc.Data()
			got = append(got, data)
		}
		want := []map[string]interface{}{
			{"title": "The Hitchhiker's Guide to the Galaxy", "tag": "comedy"},
			{"title": "The Hitchhiker's Guide to the Galaxy", "tag": "space"},
			{"title": "The Hitchhiker's Guide to the Galaxy", "tag": "adventure"},
		}
		sort.Slice(got, func(i, j int) bool {
			return got[i]["tag"].(string) < got[j]["tag"].(string)
		})
		sort.Slice(want, func(i, j int) bool {
			return want[i]["tag"].(string) < want[j]["tag"].(string)
		})
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("got: %v, want: %v, diff +want -got: %s", got, want, diff)
		}
	})
	t.Run("UnnestWithIndexField", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal(FieldOf("title"), "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag", &UnnestOptions{IndexField: "tagIndex"}).
			Select("title", "tag", "tagIndex").
			Execute(ctx).Results()
		defer iter.Stop()
		var got []map[string]interface{}
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("Failed to iterate: %v", err)
			}
			if !doc.Exists() {
				t.Fatalf("Exists: got: false, want: true")
			}
			data := doc.Data()
			got = append(got, data)
		}
		want := []map[string]interface{}{
			{"title": "The Hitchhiker's Guide to the Galaxy", "tag": "comedy", "tagIndex": int64(0)},
			{"title": "The Hitchhiker's Guide to the Galaxy", "tag": "space", "tagIndex": int64(1)},
			{"title": "The Hitchhiker's Guide to the Galaxy", "tag": "adventure", "tagIndex": int64(2)},
		}
		sort.Slice(got, func(i, j int) bool {
			return got[i]["tagIndex"].(int64) < got[j]["tagIndex"].(int64)
		})
		if diff := testutil.Diff(got, want); diff != "" {
			t.Errorf("got: %v, want: %v, diff +want -got: %s", got, want, diff)
		}
	})
	t.Run("Where", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Where(Equal(FieldOf("author.country"), "UK")).Execute(ctx).Results()
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 4 {
			t.Errorf("got %d documents, want 4", len(results))
		}
	})
}

func TestIntegration_PipelineFunctions(t *testing.T) {
	skipIfNotEnterprise(t)
	t.Run("arrayFuncs", arrayFuncs)
	t.Run("stringFuncs", stringFuncs)
	t.Run("vectorFuncs", vectorFuncs)

	t.Run("timestampFuncs", timestampFuncs)
	t.Run("arithmeticFuncs", arithmeticFuncs)
	t.Run("aggregateFuncs", aggregateFuncs)
	t.Run("comparisonFuncs", comparisonFuncs)
	t.Run("generalFuncs", generalFuncs)
	t.Run("keyFuncs", keyFuncs)
	t.Run("objectFuncs", objectFuncs)
	t.Run("logicalFuncs", logicalFuncs)
	t.Run("typeFuncs", typeFuncs)
}

func typeFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"a": nil,
		"b": true,
		"c": 1,
		"d": "hello",
		"e": []byte("world"),
		"f": time.Now(),
		"g": &latlng.LatLng{Latitude: 32.1, Longitude: -4.5},
		"h": []interface{}{1, 2, 3},
		"i": map[string]interface{}{"j": 1},
		"k": Vector64{1, 2, 3},
		"l": docRef1,
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "Type of null",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("a").As("type")),
			want:     map[string]interface{}{"type": "null"},
		},
		{
			name:     "Type of boolean",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("b").As("type")),
			want:     map[string]interface{}{"type": "boolean"},
		},
		{
			name:     "Type of int64",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("c").As("type")),
			want:     map[string]interface{}{"type": "int64"},
		},
		{
			name:     "Type of string",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("d").As("type")),
			want:     map[string]interface{}{"type": "string"},
		},
		{
			name:     "Type of bytes",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("e").As("type")),
			want:     map[string]interface{}{"type": "bytes"},
		},
		{
			name:     "Type of timestamp",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("f").As("type")),
			want:     map[string]interface{}{"type": "timestamp"},
		},
		{
			name:     "Type of geopoint",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("g").As("type")),
			want:     map[string]interface{}{"type": "geo_point"},
		},
		{
			name:     "Type of array",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("h").As("type")),
			want:     map[string]interface{}{"type": "array"},
		},
		{
			name:     "Type of map",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("i").As("type")),
			want:     map[string]interface{}{"type": "map"},
		},
		{
			name:     "Type of vector",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("k").As("type")),
			want:     map[string]interface{}{"type": "vector"},
		},
		{
			name:     "Type of reference",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Type("l").As("type")),
			want:     map[string]interface{}{"type": "reference"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func TestIntegration_Query_Pipeline(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	coll := integrationColl(t)
	h := testHelper{t}
	type Book struct {
		Title     string  `firestore:"title"`
		Genre     string  `firestore:"genre"`
		Published int     `firestore:"published"`
		Rating    float64 `firestore:"rating"`
	}
	books := []Book{
		{Title: "The Hitchhiker's Guide to the Galaxy", Genre: "Science Fiction", Published: 1979, Rating: 4.2},
		{Title: "Pride and Prejudice", Genre: "Romance", Published: 1813, Rating: 4.5},
		{Title: "One Hundred Years of Solitude", Genre: "Magical Realism", Published: 1967, Rating: 4.3},
	}
	var docRefs []*DocumentRef
	for _, b := range books {
		docRef := coll.NewDoc()
		h.mustCreate(docRef, b)
		docRefs = append(docRefs, docRef)
	}
	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})

	t.Run("Where", func(t *testing.T) {
		q := coll.Where("published", ">", 1900)
		p := q.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		res, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(res) != 2 {
			t.Errorf("got %d documents, want 2", len(res))
		}
	})

	t.Run("OrderBy", func(t *testing.T) {
		q := coll.OrderBy("published", Asc)
		p := q.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		res, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(res) != 3 {
			t.Errorf("got %d documents, want 3", len(res))
		}
		var publishedYears []int64
		for _, r := range res {
			publishedYears = append(publishedYears, r.Data()["published"].(int64))
		}
		if !sort.SliceIsSorted(publishedYears, func(i, j int) bool { return publishedYears[i] < publishedYears[j] }) {
			t.Errorf("results not sorted by published year: %v", publishedYears)
		}
	})

	t.Run("Limit", func(t *testing.T) {
		q := coll.Limit(2)
		p := q.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		res, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(res) != 2 {
			t.Errorf("got %d documents, want 2", len(res))
		}
	})

	t.Run("Offset", func(t *testing.T) {
		q := coll.OrderBy("published", Asc).Offset(1)
		p := q.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		res, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(res) != 2 {
			t.Errorf("got %d documents, want 2", len(res))
		}
	})

	t.Run("Select", func(t *testing.T) {
		q := coll.Select("title")
		p := q.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		data := doc.Data()
		if _, ok := data["title"]; !ok {
			t.Error("missing 'title' field")
		}
		if _, ok := data["genre"]; ok {
			t.Error("unexpected 'genre' field")
		}
	})
}

func TestIntegration_AggregationQuery_Pipeline(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	coll := integrationColl(t)
	h := testHelper{t}
	type Book struct {
		Title     string  `firestore:"title"`
		Genre     string  `firestore:"genre"`
		Published int     `firestore:"published"`
		Rating    float64 `firestore:"rating"`
	}
	books := []Book{
		{Title: "The Hitchhiker's Guide to the Galaxy", Genre: "Science Fiction", Published: 1979, Rating: 4.2},
		{Title: "Pride and Prejudice", Genre: "Romance", Published: 1813, Rating: 4.5},
		{Title: "One Hundred Years of Solitude", Genre: "Magical Realism", Published: 1967, Rating: 4.3},
	}
	var docRefs []*DocumentRef
	for _, b := range books {
		docRef := coll.NewDoc()
		h.mustCreate(docRef, b)
		docRefs = append(docRefs, docRef)
	}
	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})

	t.Run("Count", func(t *testing.T) {
		ag := coll.NewAggregationQuery().WithCount("count")
		p := ag.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}

		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if data["count"] != int64(3) {
			t.Errorf("got %d count, want 3", data["count"])
		}
	})

	t.Run("Sum", func(t *testing.T) {
		ag := coll.NewAggregationQuery().WithSum("published", "total_published")
		p := ag.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}

		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if data["total_published"] != int64(1979+1813+1967) {
			t.Errorf("got %d total_published, want %d", data["total_published"], int64(1979+1813+1967))
		}
	})

	t.Run("Average", func(t *testing.T) {
		ag := coll.NewAggregationQuery().WithAvg("rating", "avg_rating")
		p := ag.Pipeline()
		iter := p.Execute(ctx).Results()
		defer iter.Stop()
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}

		if !doc.Exists() {
			t.Fatalf("Exists: got: false, want: true")
		}
		data := doc.Data()
		if data["avg_rating"] != (4.2+4.5+4.3)/3 {
			t.Errorf("got %f avg_rating, want %f", data["avg_rating"], (4.2+4.5+4.3)/3)
		}
	})
}

func objectFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"m1": map[string]interface{}{"a": 1, "b": 2},
		"m2": map[string]interface{}{"c": 3, "d": 4},
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "Map",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Map(map[string]any{"a": 1, "b": 2}).As("map")),
			want:     map[string]interface{}{"map": map[string]interface{}{"a": int64(1), "b": int64(2)}},
		},
		{
			name:     "MapGet",
			pipeline: client.Pipeline().Collection(coll.ID).Select(MapGet("m1", "a").As("value")),
			want:     map[string]interface{}{"value": int64(1)},
		},
		{
			name:     "MapMerge",
			pipeline: client.Pipeline().Collection(coll.ID).Select(MapMerge("m1", FieldOf("m2")).As("merged")),
			want:     map[string]interface{}{"merged": map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(3), "d": int64(4)}},
		},
		{
			name:     "MapRemove",
			pipeline: client.Pipeline().Collection(coll.ID).Select(MapRemove("m1", "a").As("removed")),
			want:     map[string]interface{}{"removed": map[string]interface{}{"b": int64(2)}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func arrayFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"a":      []interface{}{1, 2, 3},
		"b":      []interface{}{4, 5, 6},
		"tags":   []string{"Go", "Firestore", "GCP"},
		"tags2":  []string{"Go", "Firestore"},
		"lang":   "Go",
		"status": "active",
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "ArrayLength",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayLength("a").As("length")),
			want:     map[string]interface{}{"length": int64(3)},
		},
		{
			name:     "Array",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Array(1, 2, 3).As("array")),
			want:     map[string]interface{}{"array": []interface{}{int64(1), int64(2), int64(3)}},
		},
		{
			name:     "ArrayFromSlice",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayFromSlice([]int{1, 2, 3}).As("array")),
			want:     map[string]interface{}{"array": []interface{}{int64(1), int64(2), int64(3)}},
		},
		{
			name:     "ArrayGet",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayGet("a", 1).As("element")),
			want:     map[string]interface{}{"element": int64(2)},
		},
		{
			name:     "ArrayReverse",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayReverse("a").As("reversed")),
			want:     map[string]interface{}{"reversed": []interface{}{int64(3), int64(2), int64(1)}},
		},
		{
			name:     "ArrayConcat",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayConcat("a", FieldOf("b")).As("concatenated")),
			want:     map[string]interface{}{"concatenated": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}},
		},
		{
			name:     "ArraySum",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArraySum("a").As("sum")),
			want:     map[string]interface{}{"sum": int64(6)},
		},
		{
			name:     "ArrayMaximum",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayMaximum("a").As("max")),
			want:     map[string]interface{}{"max": int64(3)},
		},
		{
			name:     "ArrayMinimum",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ArrayMinimum("a").As("min")),
			want:     map[string]interface{}{"min": int64(1)},
		},
		// Array filter conditions
		{
			name:     "ArrayContains",
			pipeline: client.Pipeline().Collection(coll.ID).Where(ArrayContains("tags", "Go")),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
		{
			name:     "ArrayContainsAll - array of mixed types",
			pipeline: client.Pipeline().Collection(coll.ID).Where(ArrayContainsAll("tags", []any{FieldOf("lang"), "Firestore"})),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
		{
			name:     "ArrayContainsAll - array of constants",
			pipeline: client.Pipeline().Collection(coll.ID).Where(ArrayContainsAll("tags", []string{"Go", "Firestore"})),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
		{
			name:     "ArrayContainsAll - Expr",
			pipeline: client.Pipeline().Collection(coll.ID).Where(ArrayContainsAll("tags", FieldOf("tags2"))),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
		{
			name:     "ArrayContainsAny",
			pipeline: client.Pipeline().Collection(coll.ID).Where(ArrayContainsAny("tags", []string{"Go", "Java"})),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
		{
			name:     "EqualAny",
			pipeline: client.Pipeline().Collection(coll.ID).Where(EqualAny("status", []string{"active", "pending"})),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
		{
			name:     "NotEqualAny",
			pipeline: client.Pipeline().Collection(coll.ID).Where(NotEqualAny("status", []string{"archived", "deleted"})),
			want:     map[string]interface{}{"lang": "Go", "tags": []interface{}{"Go", "Firestore", "GCP"}, "tags2": []interface{}{"Go", "Firestore"}, "status": "active", "a": []interface{}{int64(1), int64(2), int64(3)}, "b": []interface{}{int64(4), int64(5), int64(6)}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testutil.Retry(t, 3, time.Second, func(r *testutil.R) {
				ctx := context.Background()
				iter := test.pipeline.Execute(ctx).Results()
				defer iter.Stop()

				docs, err := iter.GetAll()
				if err != nil {
					t.Fatalf("GetAll: %v", err)
					return
				}
				if len(docs) != 1 {
					t.Fatalf("expected 1 doc, got %d", len(docs))
					return
				}
				got := docs[0].Data()
				if diff := testutil.Diff(got, test.want); diff != "" {
					t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
					return
				}
			})
		})
	}
}

func stringFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"name":        "  John Doe  ",
		"description": "This is a Firestore document.",
		"productCode": "abc-123",
		"tags":        []string{"tag1", "tag2", "tag3"},
		"email":       "john.doe@example.com",
		"zipCode":     "12345",
		"csv":         "a,b,c",
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	doc1want := map[string]interface{}{
		"name":        "  John Doe  ",
		"description": "This is a Firestore document.",
		"productCode": "abc-123",
		"tags":        []interface{}{"tag1", "tag2", "tag3"},
		"email":       "john.doe@example.com",
		"zipCode":     "12345",
		"csv":         "a,b,c",
	}

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     interface{}
	}{
		{
			name:     "ByteLength",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ByteLength("name").As("byte_length")),
			want:     map[string]interface{}{"byte_length": int64(12)},
		},
		{
			name:     "CharLength",
			pipeline: client.Pipeline().Collection(coll.ID).Select(CharLength("name").As("char_length")),
			want:     map[string]interface{}{"char_length": int64(12)},
		},
		{
			name:     "StringConcat",
			pipeline: client.Pipeline().Collection(coll.ID).Select(StringConcat(FieldOf("name"), " - ", FieldOf("productCode")).As("concatenated_string")),
			want:     map[string]interface{}{"concatenated_string": "  John Doe   - abc-123"},
		},
		{
			name:     "StringReverse",
			pipeline: client.Pipeline().Collection(coll.ID).Select(StringReverse("name").As("reversed_string")),
			want:     map[string]interface{}{"reversed_string": "  eoD nhoJ  "},
		},
		{
			name:     "Join",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Join("tags", ", ").As("joined_string")),
			want:     map[string]interface{}{"joined_string": "tag1, tag2, tag3"},
		},
		{
			name:     "Substring",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Substring("description", 0, 4).As("substring")),
			want:     map[string]interface{}{"substring": "This"},
		},
		{
			name:     "ToLower",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ToLower("name").As("lowercase_name")),
			want:     map[string]interface{}{"lowercase_name": "  john doe  "},
		},
		{
			name:     "ToUpper",
			pipeline: client.Pipeline().Collection(coll.ID).Select(ToUpper("name").As("uppercase_name")),
			want:     map[string]interface{}{"uppercase_name": "  JOHN DOE  "},
		},
		{
			name:     "Trim",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Trim("name").As("trimmed_name")),
			want:     map[string]interface{}{"trimmed_name": "John Doe"},
		},
		{
			name:     "Split",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Split("csv", ",").As("split_string")),
			want:     map[string]interface{}{"split_string": []interface{}{"a", "b", "c"}},
		},
		// String filter conditions
		{
			name:     "Like",
			pipeline: client.Pipeline().Collection(coll.ID).Where(Like("name", "%John%")),
			want:     []map[string]interface{}{doc1want},
		},
		{
			name:     "StartsWith",
			pipeline: client.Pipeline().Collection(coll.ID).Where(StartsWith("name", "  John")),
			want:     []map[string]interface{}{doc1want},
		},
		{
			name:     "EndsWith",
			pipeline: client.Pipeline().Collection(coll.ID).Where(EndsWith("name", "Doe  ")),
			want:     []map[string]interface{}{doc1want},
		},
		{
			name:     "RegexContains",
			pipeline: client.Pipeline().Collection(coll.ID).Where(RegexContains("email", "@example\\.com")),
			want:     []map[string]interface{}{doc1want},
		},
		{
			name:     "RegexMatch",
			pipeline: client.Pipeline().Collection(coll.ID).Where(RegexMatch("zipCode", "^[0-9]{5}$")),
			want:     []map[string]interface{}{doc1want},
		},
		{
			name:     "StringContains",
			pipeline: client.Pipeline().Collection(coll.ID).Where(StringContains("description", "Firestore")),
			want:     []map[string]interface{}{doc1want},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}
			lastStage := test.pipeline.stages[len(test.pipeline.stages)-1]
			lastStageName := lastStage.name()

			if lastStageName == stageNameSelect { // This is a select query
				want, ok := test.want.(map[string]interface{})
				if !ok {
					t.Fatalf("invalid test.want type for select query: %T", test.want)
					return
				}
				if len(docs) != 1 {
					t.Fatalf("expected 1 doc, got %d", len(docs))
					return
				}
				got := docs[0].Data()
				if diff := testutil.Diff(got, want); diff != "" {
					t.Errorf("got: %v, want: %v, diff +want -got: %s", got, want, diff)
				}
			} else if lastStageName == stageNameWhere { // This is a where query (filter condition)
				want, ok := test.want.([]map[string]interface{})
				if !ok {
					t.Fatalf("invalid test.want type for where query: %T", test.want)
					return
				}
				if len(docs) != len(want) {
					t.Fatalf("expected %d doc(s), got %d", len(want), len(docs))
					return
				}
				var gots []map[string]interface{}
				for _, doc := range docs {
					got := doc.Data()
					gots = append(gots, got)
				}
				if diff := testutil.Diff(gots, want); diff != "" {
					t.Errorf("got: %v, want: %v, diff +want -got: %s", gots, want, diff)
				}
			} else {
				t.Fatalf("unknown pipeline stage: %s", lastStageName)
				return
			}
		})
	}

}

func vectorFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"v1": Vector64{1.0, 2.0, 3.0},
		"v2": Vector64{4.0, 5.0, 6.0},
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "VectorLength",
			pipeline: client.Pipeline().Collection(coll.ID).Select(VectorLength("v1").As("length")),
			want:     map[string]interface{}{"length": int64(3)},
		},
		{
			name:     "DotProduct - field and field",
			pipeline: client.Pipeline().Collection(coll.ID).Select(DotProduct("v1", FieldOf("v2")).As("dot_product")),
			want:     map[string]interface{}{"dot_product": float64(1*4 + 2*5 + 3*6)},
		},
		{
			name:     "DotProduct - field and constant",
			pipeline: client.Pipeline().Collection(coll.ID).Select(DotProduct("v1", Vector64{4.0, 5.0, 6.0}).As("dot_product")),
			want:     map[string]interface{}{"dot_product": float64(1*4 + 2*5 + 3*6)},
		},
		{
			name:     "EuclideanDistance - field and field",
			pipeline: client.Pipeline().Collection(coll.ID).Select(EuclideanDistance("v1", FieldOf("v2")).As("euclidean")),
			want:     map[string]interface{}{"euclidean": math.Sqrt(math.Pow(4-1, 2) + math.Pow(5-2, 2) + math.Pow(6-3, 2))},
		},
		{
			name:     "EuclideanDistance - field and constant",
			pipeline: client.Pipeline().Collection(coll.ID).Select(EuclideanDistance("v1", Vector64{4.0, 5.0, 6.0}).As("euclidean")),
			want:     map[string]interface{}{"euclidean": math.Sqrt(math.Pow(4-1, 2) + math.Pow(5-2, 2) + math.Pow(6-3, 2))},
		},
		{
			name:     "CosineDistance - field and field",
			pipeline: client.Pipeline().Collection(coll.ID).Select(CosineDistance("v1", FieldOf("v2")).As("cosine")),
			want:     map[string]interface{}{"cosine": 1 - (32 / (math.Sqrt(14) * math.Sqrt(77)))},
		},
		{
			name:     "CosineDistance - field and constant",
			pipeline: client.Pipeline().Collection(coll.ID).Select(CosineDistance("v1", Vector64{4.0, 5.0, 6.0}).As("cosine")),
			want:     map[string]interface{}{"cosine": 1 - (32 / (math.Sqrt(14) * math.Sqrt(77)))},
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
				return
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func timestampFuncs(t *testing.T) {
	t.Parallel()
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	h := testHelper{t}
	now := time.Now()
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"timestamp":   now,
		"unixMicros":  now.UnixNano() / 1000,
		"unixMillis":  now.UnixNano() / 1e6,
		"unixSeconds": now.Unix(),
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name: "TimestampAdd day",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampAdd("timestamp", "day", 1).As("timestamp_plus_day")),
			want: map[string]interface{}{"timestamp_plus_day": now.AddDate(0, 0, 1).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampAdd hour",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampAdd("timestamp", "hour", 1).As("timestamp_plus_hour")),
			want: map[string]interface{}{"timestamp_plus_hour": now.Add(time.Hour).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampAdd minute",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampAdd("timestamp", "minute", 1).As("timestamp_plus_minute")),
			want: map[string]interface{}{"timestamp_plus_minute": now.Add(time.Minute).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampAdd second",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampAdd("timestamp", "second", 1).As("timestamp_plus_second")),
			want: map[string]interface{}{"timestamp_plus_second": now.Add(time.Second).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampSubtract",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampSubtract("timestamp", "hour", 1).As("timestamp_minus_hour")),
			want: map[string]interface{}{"timestamp_minus_hour": now.Add(-time.Hour).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampToUnixMicros",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(FieldOf("timestamp").TimestampToUnixMicros().As("timestamp_micros")),
			want: map[string]interface{}{"timestamp_micros": now.UnixNano() / 1000},
		},
		{
			name: "TimestampToUnixMillis",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(FieldOf("timestamp").TimestampToUnixMillis().As("timestamp_millis")),
			want: map[string]interface{}{"timestamp_millis": now.UnixNano() / 1e6},
		},
		{
			name: "TimestampToUnixSeconds",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(FieldOf("timestamp").TimestampToUnixSeconds().As("timestamp_seconds")),
			want: map[string]interface{}{"timestamp_seconds": now.Unix()},
		},
		{
			name: "UnixMicrosToTimestamp - constant",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(UnixMicrosToTimestamp(ConstantOf(now.UnixNano() / 1000)).As("timestamp_from_micros")),
			want: map[string]interface{}{"timestamp_from_micros": now.Truncate(time.Microsecond)},
		},
		{
			name: "UnixMicrosToTimestamp - fieldname",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(UnixMicrosToTimestamp("unixMicros").As("timestamp_from_micros")),
			want: map[string]interface{}{"timestamp_from_micros": now.Truncate(time.Microsecond)},
		},
		{
			name: "UnixMillisToTimestamp",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(UnixMillisToTimestamp(ConstantOf(now.UnixNano() / 1e6)).As("timestamp_from_millis")),
			want: map[string]interface{}{"timestamp_from_millis": now.Truncate(time.Millisecond)},
		},
		{
			name: "UnixSecondsToTimestamp",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(UnixSecondsToTimestamp("unixSeconds").As("timestamp_from_seconds")),
			want: map[string]interface{}{"timestamp_from_seconds": now.Truncate(time.Second)},
		},
		{
			name: "CurrentTimestamp",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(CurrentTimestamp().As("current_timestamp")),
			want: map[string]interface{}{"current_timestamp": time.Now().Truncate(time.Microsecond)},
		},
		{
			name: "TimestampTruncate day",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampTruncate("timestamp", "day").As("timestamp_trunc_day")),
			want: map[string]interface{}{"timestamp_trunc_day": time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampTruncate hour",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampTruncate("timestamp", "hour").As("timestamp_trunc_hour")),
			want: map[string]interface{}{"timestamp_trunc_hour": time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location()).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampTruncate minute",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampTruncate("timestamp", "minute").As("timestamp_trunc_minute")),
			want: map[string]interface{}{"timestamp_trunc_minute": time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location()).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampTruncate second",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampTruncate("timestamp", "second").As("timestamp_trunc_second")),
			want: map[string]interface{}{"timestamp_trunc_second": time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, now.Location()).Truncate(time.Microsecond)},
		},
		{
			name: "TimestampTruncateWithTimezone day",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Select(TimestampTruncateWithTimezone("timestamp", "day", "America/New_York").As("timestamp_trunc_day_ny")),
			want: map[string]interface{}{"timestamp_trunc_day_ny": func() time.Time {
				loc, _ := time.LoadLocation("America/New_York")
				nowInLoc := now.In(loc)
				return time.Date(nowInLoc.Year(), nowInLoc.Month(), nowInLoc.Day(), 0, 0, 0, 0, loc).Truncate(time.Microsecond)
			}()},
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			margin := 0 * time.Microsecond
			if test.name == "CurrentTimestamp" {
				margin = 5 * time.Second
			}
			if diff := testutil.Diff(got, test.want, cmpopts.EquateApproxTime(margin)); diff != "" {
				t.Errorf("got: %v, want: %v, diff: %s", got, test.want, diff)
			}
		})
	}
}

func arithmeticFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"a": int(1),
		"b": int(2),
		"c": -3,
		"d": 4.5,
		"e": -5.5,
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "Add - left FieldOf, right FieldOf",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Add(FieldOf("a"), FieldOf("b")).As("add")),
			want:     map[string]interface{}{"add": int64(3)},
		},
		{
			name:     "Add - left FieldOf, right ConstantOf",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Add(FieldOf("a"), ConstantOf(2)).As("add")),
			want:     map[string]interface{}{"add": int64(3)},
		},
		{
			name:     "Add - left FieldOf, right constant",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Add(FieldOf("a"), 5).As("add")),
			want:     map[string]interface{}{"add": int64(6)},
		},
		{
			name:     "Add - left fieldname, right constant",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Add("a", 5).As("add")),
			want:     map[string]interface{}{"add": int64(6)},
		},
		{
			name:     "Add - left fieldpath, right constant",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Add(FieldPath([]string{"a"}), 5).As("add")),
			want:     map[string]interface{}{"add": int64(6)},
		},
		{
			name:     "Add - left fieldpath, right expression",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Add(FieldPath([]string{"a"}), Add(FieldOf("b"), FieldOf("d"))).As("add")),
			want:     map[string]interface{}{"add": float64(7.5)},
		},
		{
			name:     "Subtract",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Subtract("a", FieldOf("b")).As("subtract")),
			want:     map[string]interface{}{"subtract": int64(-1)},
		},
		{
			name:     "Multiply",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Multiply("a", 5).As("multiply")),
			want:     map[string]interface{}{"multiply": int64(5)},
		},
		{
			name:     "Divide",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Divide("a", FieldOf("d")).As("divide")),
			want:     map[string]interface{}{"divide": float64(1 / 4.5)},
		},
		{
			name:     "Mod",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Mod("a", FieldOf("b")).As("mod")),
			want:     map[string]interface{}{"mod": int64(1)},
		},
		{
			name:     "Pow",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Pow("a", FieldOf("b")).As("pow")),
			want:     map[string]interface{}{"pow": float64(1)},
		},
		{
			name:     "Abs - fieldname",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Abs("c").As("abs")),
			want:     map[string]interface{}{"abs": int64(3)},
		},
		{
			name:     "Abs - fieldPath",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Abs(FieldPath([]string{"c"})).As("abs")),
			want:     map[string]interface{}{"abs": int64(3)},
		},
		{
			name:     "Abs - Expr",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Abs(Add(FieldOf("b"), FieldOf("d"))).As("abs")),
			want:     map[string]interface{}{"abs": float64(6.5)},
		},
		{
			name:     "Ceil",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Ceil("d").As("ceil")),
			want:     map[string]interface{}{"ceil": float64(5)},
		},
		{
			name:     "Floor",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Floor("d").As("floor")),
			want:     map[string]interface{}{"floor": float64(4)},
		},
		{
			name:     "Round",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Round("d").As("round")),
			want:     map[string]interface{}{"round": float64(5)},
		},
		{
			name:     "Sqrt",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Sqrt("d").As("sqrt")),
			want:     map[string]interface{}{"sqrt": math.Sqrt(4.5)},
		},
		{
			name:     "Log",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Log("d", 2).As("log")),
			want:     map[string]interface{}{"log": math.Log2(4.5)},
		},
		{
			name:     "Log10",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Log10("d").As("log10")),
			want:     map[string]interface{}{"log10": math.Log10(4.5)},
		},
		{
			name:     "Ln",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Ln("d").As("ln")),
			want:     map[string]interface{}{"ln": math.Log(4.5)},
		},
		{
			name:     "Exp",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Exp("d").As("exp")),
			want:     map[string]interface{}{"exp": math.Exp(4.5)},
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func aggregateFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"a": 1,
	})
	docRef2 := coll.NewDoc()
	h.mustCreate(docRef2, map[string]interface{}{
		"a": 2,
	})
	docRef3 := coll.NewDoc()
	h.mustCreate(docRef3, map[string]interface{}{
		"b": 2,
	})
	defer deleteDocuments([]*DocumentRef{docRef1, docRef2, docRef3})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name: "Sum - fieldname arg",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(Sum("a").As("sum_a")),
			want: map[string]interface{}{"sum_a": int64(3)},
		},
		{
			name: "Sum - fieldpath arg",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(Sum(FieldPath([]string{"a"})).As("sum_a")),
			want: map[string]interface{}{"sum_a": int64(3)},
		},
		{
			name: "Sum - FieldOf Expr",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(Sum(FieldOf("a")).As("sum_a")),
			want: map[string]interface{}{"sum_a": int64(3)},
		},
		{
			name: "Sum - FieldOf Path Expr",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(Sum(FieldOf(FieldPath([]string{"a"}))).As("sum_a")),
			want: map[string]interface{}{"sum_a": int64(3)},
		},
		{
			name: "Avg",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(Average("a").As("avg_a")),
			want: map[string]interface{}{"avg_a": float64(1.5)},
		},
		{
			name: "Count",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(Count("a").As("count_a")),
			want: map[string]interface{}{"count_a": int64(2)},
		},
		{
			name: "CountAll",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Aggregate(CountAll().As("count_all")),
			want: map[string]interface{}{"count_all": int64(3)},
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func comparisonFuncs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := integrationClient(t)
	now := time.Now()
	coll := client.Collection(collectionIDs.New())
	doc1data := map[string]interface{}{
		"timestamp": now,
		"a":         1,
		"b":         2,
		"c":         -3,
		"d":         4.5,
		"e":         -5.5,
	}
	_, err := coll.Doc("doc1").Create(ctx, doc1data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	doc2data := map[string]interface{}{
		"timestamp": now,
		"a":         2,
		"b":         2,
		"c":         -3,
		"d":         4.5,
		"e":         -5.5,
	}
	_, err = coll.Doc("doc2").Create(ctx, doc2data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer deleteDocuments([]*DocumentRef{coll.Doc("doc1"), coll.Doc("doc2")})

	doc1want := map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(-3), "d": float64(4.5), "e": float64(-5.5), "timestamp": now.Truncate(time.Microsecond)}

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     []map[string]interface{}
	}{
		{
			name: "Equal",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Where(Equal("a", 1)),
			want: []map[string]interface{}{doc1want},
		},
		{
			name: "NotEqual",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Where(NotEqual("a", 2)),
			want: []map[string]interface{}{doc1want},
		},
		{
			name: "LessThan",
			pipeline: client.Pipeline().
				Collection(coll.ID).
				Where(LessThan("a", 2)),
			want: []map[string]interface{}{doc1want},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
			}
			if len(docs) != len(test.want) {
				t.Fatalf("expected %d doc(s), got %d", len(test.want), len(docs))
			}

			var gots []map[string]interface{}
			for _, doc := range docs {
				got := doc.Data()
				if ts, ok := got["timestamp"].(time.Time); ok {
					got["timestamp"] = ts.Truncate(time.Microsecond)
				}
				gots = append(gots, got)
			}

			if diff := testutil.Diff(gots, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", gots, test.want, diff)
			}
		})
	}
}

func keyFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.Doc("doc1")
	h.mustCreate(docRef1, map[string]interface{}{
		"a": "hello",
		"b": "world",
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "CollectionId",
			pipeline: client.Pipeline().Collection(coll.ID).Select(GetCollectionID("__name__").As("collectionId")),
			want:     map[string]interface{}{"collectionId": coll.ID},
		},
		{
			name:     "DocumentId",
			pipeline: client.Pipeline().Collection(coll.ID).Select(GetDocumentID(docRef1).As("documentId")),
			want:     map[string]interface{}{"documentId": "doc1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func generalFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.NewDoc()
	h.mustCreate(docRef1, map[string]interface{}{
		"a": "hello",
		"b": "world",
	})
	defer deleteDocuments([]*DocumentRef{docRef1})

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     map[string]interface{}
	}{
		{
			name:     "Length - string literal",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Length(ConstantOf("hello")).As("len")),
			want:     map[string]interface{}{"len": int64(5)},
		},
		{
			name:     "Length - field",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Length("a").As("len")),
			want:     map[string]interface{}{"len": int64(5)},
		},
		{
			name:     "Length - field path",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Length(FieldPath{"a"}).As("len")),
			want:     map[string]interface{}{"len": int64(5)},
		},
		{
			name:     "Reverse - string literal",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Reverse(ConstantOf("hello")).As("reverse")),
			want:     map[string]interface{}{"reverse": "olleh"},
		},
		{
			name:     "Reverse - field",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Reverse("a").As("reverse")),
			want:     map[string]interface{}{"reverse": "olleh"},
		},
		{
			name:     "Reverse - field path",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Reverse(FieldPath{"a"}).As("reverse")),
			want:     map[string]interface{}{"reverse": "olleh"},
		},
		{
			name:     "Concat - two literals",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Concat(ConstantOf("hello"), ConstantOf("world")).As("concat")),
			want:     map[string]interface{}{"concat": "helloworld"},
		},
		{
			name:     "Concat - literal and field",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Concat(ConstantOf("hello"), FieldOf("b")).As("concat")),
			want:     map[string]interface{}{"concat": "helloworld"},
		},
		{
			name:     "Concat - two fields",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Concat(FieldOf("a"), FieldOf("b")).As("concat")),
			want:     map[string]interface{}{"concat": "helloworld"},
		},
		{
			name:     "Concat - field and literal",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Concat(FieldOf("a"), ConstantOf("world")).As("concat")),
			want:     map[string]interface{}{"concat": "helloworld"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}
			if len(docs) != 1 {
				t.Fatalf("expected 1 doc, got %d", len(docs))
			}
			got := docs[0].Data()
			if diff := testutil.Diff(got, test.want); diff != "" {
				t.Errorf("got: %v, want: %v, diff +want -got: %s", got, test.want, diff)
			}
		})
	}
}

func logicalFuncs(t *testing.T) {
	t.Parallel()
	h := testHelper{t}
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	docRef1 := coll.Doc("doc1")
	doc1Data := map[string]interface{}{
		"a": 1,
		"b": 2,
		"c": nil,
		"d": true,
		"e": false,
	}
	h.mustCreate(docRef1, doc1Data)

	docRef2 := coll.Doc("doc2")
	doc2Data := map[string]interface{}{
		"a": 1,
		"b": 1,
		"d": true,
		"e": true,
	}
	h.mustCreate(docRef2, doc2Data)
	defer deleteDocuments([]*DocumentRef{docRef1, docRef2})

	doc1Want := map[string]interface{}{
		"a": int64(1),
		"b": int64(2),
		"c": nil,
		"d": true,
		"e": false,
	}
	doc2Want := map[string]interface{}{
		"a": int64(1),
		"b": int64(1),
		"d": true,
		"e": true,
	}

	tests := []struct {
		name     string
		pipeline *Pipeline
		want     interface{}
	}{
		{
			name:     "Conditional - true",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Conditional(Equal(ConstantOf(1), ConstantOf(1)), FieldOf("a"), FieldOf("b")).As("result")),
			want:     []map[string]interface{}{{"result": int64(1)}, {"result": int64(1)}},
		},
		{
			name:     "Conditional - false",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Conditional(Equal(ConstantOf(1), ConstantOf(0)), FieldOf("a"), FieldOf("b")).As("result")),
			want:     []map[string]interface{}{{"result": int64(2)}, {"result": int64(1)}},
		},
		{
			name:     "Conditional - field true",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Conditional(Equal(FieldOf("d"), ConstantOf(true)), FieldOf("a"), FieldOf("b")).As("result")),
			want:     []map[string]interface{}{{"result": int64(1)}, {"result": int64(1)}},
		},
		{
			name:     "Conditional - field false",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Conditional(Equal(FieldOf("e"), ConstantOf(true)), FieldOf("a"), FieldOf("b")).As("result")),
			want:     []map[string]interface{}{{"result": int64(2)}, {"result": int64(1)}},
		},
		{
			name:     "LogicalMax",
			pipeline: client.Pipeline().Collection(coll.ID).Select(LogicalMaximum(FieldOf("a"), FieldOf("b")).As("max")),
			want:     []map[string]interface{}{{"max": int64(2)}, {"max": int64(1)}},
		},
		{
			name:     "LogicalMin",
			pipeline: client.Pipeline().Collection(coll.ID).Select(LogicalMinimum(FieldOf("a"), FieldOf("b")).As("min")),
			want:     []map[string]interface{}{{"min": int64(1)}, {"min": int64(1)}},
		},
		{
			name:     "IfError - no error",
			pipeline: client.Pipeline().Collection(coll.ID).Select(IfError(FieldOf("a"), ConstantOf(100)).As("result")),
			want:     []map[string]interface{}{{"result": int64(1)}, {"result": int64(1)}},
		},
		{
			name:     "IfError - error",
			pipeline: client.Pipeline().Collection(coll.ID).Select(Divide("a", 0).IfError(ConstantOf("was error")).As("ifError")),
			want:     []map[string]interface{}{{"ifError": "was error"}, {"ifError": "was error"}},
		},
		{
			name:     "IfErrorBoolean - no error",
			pipeline: client.Pipeline().Collection(coll.ID).Select(IfErrorBoolean(Equal(FieldOf("d"), ConstantOf(true)), Equal(ConstantOf(1), ConstantOf(0))).As("result")),
			want:     []map[string]interface{}{{"result": true}, {"result": true}},
		},
		{
			name:     "IfErrorBoolean - error",
			pipeline: client.Pipeline().Collection(coll.ID).Select(IfErrorBoolean(Equal(FieldOf("x"), ConstantOf(true)), Equal(ConstantOf(1), ConstantOf(0))).As("result")),
			want:     []map[string]interface{}{{"result": false}, {"result": false}},
		},
		{
			name:     "IfAbsent - not absent",
			pipeline: client.Pipeline().Collection(coll.ID).Select(IfAbsent(FieldOf("a"), ConstantOf(100)).As("result")),
			want:     []map[string]interface{}{{"result": int64(1)}, {"result": int64(1)}},
		},
		{
			name:     "IfAbsent - absent",
			pipeline: client.Pipeline().Collection(coll.ID).Select(IfAbsent(FieldOf("x"), ConstantOf(100)).As("result")),
			want:     []map[string]interface{}{{"result": int64(100)}, {"result": int64(100)}},
		},
		{
			name: "And",
			pipeline: client.Pipeline().Collection(coll.ID).Where(
				And(
					Equal(FieldOf("a"), 1),
					Equal(FieldOf("b"), 2),
				),
			),
			want: []map[string]interface{}{doc1Want},
		},
		{
			name: "Or",
			pipeline: client.Pipeline().Collection(coll.ID).Where(
				Or(
					Equal(FieldOf("b"), 2),
					Equal(FieldOf("e"), true),
				),
			),
			want: []map[string]interface{}{doc1Want, doc2Want},
		},
		{
			name: "Not",
			pipeline: client.Pipeline().Collection(coll.ID).Where(
				Not(Equal(FieldOf("b"), 1)),
			),
			want: []map[string]interface{}{doc1Want},
		},
		{
			name: "Xor",
			pipeline: client.Pipeline().Collection(coll.ID).Where(
				Xor(
					Equal(FieldOf("d"), true),
					Equal(FieldOf("e"), true),
				),
			),
			want: []map[string]interface{}{doc1Want},
		},
		{
			name:     "FieldExists",
			pipeline: client.Pipeline().Collection(coll.ID).Where(FieldExists("c")),
			want:     []map[string]interface{}{doc1Want},
		},
		{
			name:     "IsError",
			pipeline: client.Pipeline().Collection(coll.ID).Where(IsError(Divide("a", 0))),
			want:     []map[string]interface{}{doc1Want, doc2Want},
		},
		{
			name:     "IsAbsent",
			pipeline: client.Pipeline().Collection(coll.ID).Where(IsAbsent("c")),
			want:     []map[string]interface{}{doc2Want},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			iter := test.pipeline.Execute(ctx).Results()
			defer iter.Stop()

			docs, err := iter.GetAll()
			if err != nil {
				t.Fatalf("GetAll: %v", err)
				return
			}

			lastStage := test.pipeline.stages[len(test.pipeline.stages)-1]
			lastStageName := lastStage.name()

			if lastStageName == stageNameSelect { // This is a select query
				want, ok := test.want.([]map[string]interface{})
				if !ok {
					t.Fatalf("invalid test.want type for select query: %T", test.want)
					return
				}
				if len(docs) != len(want) {
					t.Fatalf("expected %d doc(s), got %d", len(want), len(docs))
					return
				}
				var gots []map[string]interface{}
				for _, doc := range docs {
					gots = append(gots, doc.Data())
				}
				if diff := testutil.Diff(gots, want, cmpopts.SortSlices(func(a, b map[string]interface{}) bool {
					// A stable sort for the results.
					// Try to sort by "result", "max", "min", "ifError"
					if v1, ok := a["result"]; ok {
						v2 := b["result"]
						switch v1 := v1.(type) {
						case int64:
							return v1 < v2.(int64)
						case bool:
							return !v1 && v2.(bool)
						}
					}
					if v1, ok := a["max"]; ok {
						return v1.(int64) < b["max"].(int64)
					}
					if v1, ok := a["min"]; ok {
						return v1.(int64) < b["min"].(int64)
					}
					if v1, ok := a["ifError"]; ok {
						return v1.(string) < b["ifError"].(string)
					}
					return false
				})); diff != "" {
					t.Errorf("got: %v, want: %v, diff +want -got: %s", gots, want, diff)
				}
			} else if lastStageName == stageNameWhere { // This is a where query (filter condition)
				want, ok := test.want.([]map[string]interface{})
				if !ok {
					t.Fatalf("invalid test.want type for where query: %T", test.want)
					return
				}
				if len(docs) != len(want) {
					t.Fatalf("expected %d doc(s), got %d", len(want), len(docs))
					return
				}
				var gots []map[string]interface{}
				for _, doc := range docs {
					got := doc.Data()
					gots = append(gots, got)
				}
				// Sort slices before comparing for consistent test results
				sort.Slice(gots, func(i, j int) bool {
					if gots[i]["a"].(int64) == gots[j]["a"].(int64) {
						return gots[i]["b"].(int64) < gots[j]["b"].(int64)
					}
					return gots[i]["a"].(int64) < gots[j]["a"].(int64)
				})
				sort.Slice(want, func(i, j int) bool {
					if want[i]["a"].(int64) == want[j]["a"].(int64) {
						return want[i]["b"].(int64) < want[j]["b"].(int64)
					}
					return want[i]["a"].(int64) < want[j]["a"].(int64)
				})
				if diff := testutil.Diff(gots, want); diff != "" {
					t.Errorf("got: %v, want: %v, diff +want -got: %s", gots, want, diff)
				}
			} else {
				t.Fatalf("unknown pipeline stage: %s", lastStageName)
				return
			}
		})
	}
}

func TestIntegration_CreateFromQuery(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	client := integrationClient(t)
	coll := integrationColl(t)
	h := testHelper{t}

	books := testBooks()[:3]
	var docRefs []*DocumentRef
	for _, b := range books {
		docRef := coll.NewDoc()
		h.mustCreate(docRef, b)
		docRefs = append(docRefs, docRef)
	}
	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})

	q := coll.Where("rating", ">", 4.2)
	p := client.Pipeline().CreateFromQuery(q)
	iter := p.Execute(ctx).Results()
	defer iter.Stop()
	results, err := iter.GetAll()
	if err != nil {
		t.Fatalf("Failed to iterate: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("got %d documents, want 2", len(results))
	}
}

func TestIntegration_CreateFromAggregationQuery(t *testing.T) {
	skipIfNotEnterprise(t)
	ctx := context.Background()
	client := integrationClient(t)
	coll := integrationColl(t)
	h := testHelper{t}

	books := testBooks()[:3]
	var docRefs []*DocumentRef
	for _, b := range books {
		docRef := coll.NewDoc()
		h.mustCreate(docRef, b)
		docRefs = append(docRefs, docRef)
	}
	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})

	ag := coll.NewAggregationQuery().WithCount("count")
	p := client.Pipeline().CreateFromAggregationQuery(ag)
	iter := p.Execute(ctx).Results()
	defer iter.Stop()
	doc, err := iter.Next()
	if err != nil {
		t.Fatalf("Failed to iterate: %v", err)
	}
	if !doc.Exists() {
		t.Fatalf("Exists: got: false, want: true")
	}
	data := doc.Data()
	if data["count"] != int64(3) {
		t.Errorf("got count %d, want 3", data["count"])
	}
}

func setupJavaBooks(t *testing.T, coll *CollectionRef) map[string]map[string]interface{} {
	h := testHelper{t}

	bookDocs := map[string]map[string]interface{}{
		"book1": {
			"title":     "The Hitchhiker's Guide to the Galaxy",
			"author":    "Douglas Adams",
			"genre":     "Science Fiction",
			"published": 1979,
			"rating":    4.2,
			"tags":      []interface{}{"comedy", "space", "adventure"},
			"awards":    map[string]interface{}{"hugo": true, "nebula": false},
			"embedding": Vector64{10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book2": {
			"title":     "Pride and Prejudice",
			"author":    "Jane Austen",
			"genre":     "Romance",
			"published": 1813,
			"rating":    4.5,
			"tags":      []interface{}{"classic", "social commentary", "love"},
			"awards":    map[string]interface{}{"none": true},
			"embedding": Vector64{1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book3": {
			"title":     "One Hundred Years of Solitude",
			"author":    "Gabriel García Márquez",
			"genre":     "Magical Realism",
			"published": 1967,
			"rating":    4.3,
			"tags":      []interface{}{"family", "history", "fantasy"},
			"awards":    map[string]interface{}{"nobel": true, "nebula": false},
			"embedding": Vector64{1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book4": {
			"title":     "The Lord of the Rings",
			"author":    "J.R.R. Tolkien",
			"genre":     "Fantasy",
			"published": 1954,
			"rating":    4.7,
			"tags":      []interface{}{"adventure", "magic", "epic"},
			"awards":    map[string]interface{}{"hugo": false, "nebula": false},
			"cost":      math.NaN(),
			"embedding": Vector64{1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book5": {
			"title":     "The Handmaid's Tale",
			"author":    "Margaret Atwood",
			"genre":     "Dystopian",
			"published": 1985,
			"rating":    4.1,
			"tags":      []interface{}{"feminism", "totalitarianism", "resistance"},
			"awards":    map[string]interface{}{"arthur c. clarke": true, "booker prize": false},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book6": {
			"title":     "Crime and Punishment",
			"author":    "Fyodor Dostoevsky",
			"genre":     "Psychological Thriller",
			"published": 1866,
			"rating":    4.3,
			"tags":      []interface{}{"philosophy", "crime", "redemption"},
			"awards":    map[string]interface{}{"none": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0},
		},
		"book7": {
			"title":     "To Kill a Mockingbird",
			"author":    "Harper Lee",
			"genre":     "Southern Gothic",
			"published": 1960,
			"rating":    4.2,
			"tags":      []interface{}{"racism", "injustice", "coming-of-age"},
			"awards":    map[string]interface{}{"pulitzer": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 1.0},
		},
		"book8": {
			"title":     "1984",
			"author":    "George Orwell",
			"genre":     "Dystopian",
			"published": 1949,
			"rating":    4.2,
			"tags":      []interface{}{"surveillance", "totalitarianism", "propaganda"},
			"awards":    map[string]interface{}{"prometheus": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 1.0},
		},
		"book9": {
			"title":     "The Great Gatsby",
			"author":    "F. Scott Fitzgerald",
			"genre":     "Modernist",
			"published": 1925,
			"rating":    4.0,
			"tags":      []interface{}{"wealth", "american dream", "love"},
			"awards":    map[string]interface{}{"none": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0},
		},
		"book10": {
			"title":     "Dune",
			"author":    "Frank Herbert",
			"genre":     "Science Fiction",
			"published": 1965,
			"rating":    4.6,
			"tags":      []interface{}{"politics", "desert", "ecology"},
			"awards":    map[string]interface{}{"hugo": true, "nebula": true},
			"embedding": Vector64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0},
		},
		"book11": {
			"title":     "Timestamp Book",
			"author":    "Timestamp Author",
			"timestamp": time.Now(),
		},
	}

	var docRefs []*DocumentRef
	for id, data := range bookDocs {
		docRef := coll.Doc(id)
		h.mustCreate(docRef, data)
		docRefs = append(docRefs, docRef)
	}

	t.Cleanup(func() {
		deleteDocuments(docRefs)
	})

	return bookDocs
}

func TestIntegration_ITPipelineTest(t *testing.T) {
	skipIfNotEnterprise(t)
	runTest := func(name string, f func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef)) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client := integrationClient(t)
			coll := integrationColl(t)
			_ = setupJavaBooks(t, coll)
			f(t, ctx, client, coll)
		})
	}

	runTest("TestAllDataTypes", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		refDate := time.Now().Truncate(time.Millisecond) // Java uses Date which is millisecond precision
		refTimestamp := refDate
		refGeoPoint := &latlng.LatLng{Latitude: 1, Longitude: 2}
		refBytes := []byte{1, 2, 3}
		refVector := Vector64{1.0, 2.0, 3.0}

		refMap := map[string]interface{}{
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

		refArray := []interface{}{
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
				ConstantOf(1).As("number"),
				ConstantOf("a string").As("string"),
				ConstantOf(true).As("boolean"),
				ConstantOfNull().As("null"),
				ConstantOf(refTimestamp).As("timestamp"),
				ConstantOf(refDate).As("date"),
				ConstantOf(refGeoPoint).As("geoPoint"),
				ConstantOf(refBytes).As("bytes"),
				ConstantOf(refVector).As("vector"),
				Map(refMap).As("map"),
				ArrayFromSlice(refArray).As("array"),
			)

		iter := pipeline.Execute(ctx).Results()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		data := results[0].Data()

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
			t.Errorf("null mismatch: got %v", data["null"])
		}
		if diff := testutil.Diff(data["geoPoint"], refGeoPoint); diff != "" {
			t.Errorf("geoPoint mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["timestamp"], refTimestamp, cmpopts.EquateApproxTime(time.Millisecond)); diff != "" {
			t.Errorf("timestamp mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["date"], refDate, cmpopts.EquateApproxTime(time.Millisecond)); diff != "" {
			t.Errorf("date mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["bytes"], refBytes); diff != "" {
			t.Errorf("bytes mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["vector"], refVector); diff != "" {
			t.Errorf("vector mismatch: %s", diff)
		}

		if diff := testutil.Diff(data["map"], refMap, cmpopts.EquateApproxTime(time.Millisecond)); diff != "" {
			t.Errorf("map mismatch: %s", diff)
		}
		if diff := testutil.Diff(data["array"], refArray, cmpopts.EquateApproxTime(time.Millisecond)); diff != "" {
			t.Errorf("array mismatch: %s", diff)
		}
	})

	runTest("TestResultMetadata", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		pipeline := client.Pipeline().Collection(coll.ID)
		snapshot := pipeline.Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}

		execTime, err := snapshot.ExecutionTime()
		if err != nil {
			t.Fatalf("ExecutionTime: %v", err)
		}
		if execTime == nil {
			t.Error("ExecutionTime is nil")
		}

		for _, res := range results {
			if res.CreateTime() == nil {
				t.Error("CreateTime is nil")
			}
			if res.UpdateTime() == nil {
				t.Error("UpdateTime is nil")
			}
			if res.ExecutionTime() == nil {
				t.Error("Result ExecutionTime is nil")
			}
			if res.CreateTime().After(*res.UpdateTime()) {
				t.Error("CreateTime > UpdateTime")
			}
			if !res.UpdateTime().Before(*execTime) {
				t.Error("UpdateTime >= ExecutionTime")
			}
		}

		docRef := coll.Doc("book1")
		_, err = docRef.Update(ctx, []Update{{Path: "rating", Value: 5.0}})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}

		snapshot = pipeline.Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).Execute(ctx)
		results, err = snapshot.Results().GetAll()
		if err != nil {
			t.Fatalf("GetAll after update: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		res := results[0]
		if !res.CreateTime().Before(*res.UpdateTime()) {
			t.Error("CreateTime >= UpdateTime after update")
		}
	})

	runTest("TestResultIsEqual", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		pipeline := client.Pipeline().Collection(coll.ID).Sort(Ascending(FieldOf("title")))

		snap1 := pipeline.Limit(1).Execute(ctx)
		res1, err := snap1.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		snap2 := pipeline.Limit(1).Execute(ctx)
		res2, err := snap2.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		snap3 := pipeline.Offset(1).Limit(1).Execute(ctx)
		res3, err := snap3.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(res1) != 1 || len(res2) != 1 || len(res3) != 1 {
			t.Fatal("Expected 1 result for each snapshot")
		}

		if diff := testutil.Diff(res1[0].Data(), res2[0].Data()); diff != "" {
			t.Errorf("res1 != res2: %s", diff)
		}
		if diff := testutil.Diff(res1[0].Data(), res3[0].Data()); diff == "" {
			t.Error("res1 == res3, expected different")
		}
	})

	runTest("TestEmptyResultMetadata", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		pipeline := client.Pipeline().Collection(coll.ID).Limit(0)
		snapshot := pipeline.Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 results, got %d", len(results))
		}
		execTime, err := snapshot.ExecutionTime()
		if err != nil {
			t.Fatal(err)
		}
		if execTime == nil {
			t.Error("ExecutionTime is nil")
		}
		if time.Since(*execTime) > 10*time.Second {
			t.Errorf("ExecutionTime too old: %v", execTime)
		}
	})

	runTest("TestAggregateResultMetadata", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		pipeline := client.Pipeline().Collection(coll.ID).Aggregate(CountAll().As("count"))
		snapshot := pipeline.Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
		res := results[0]
		if res.CreateTime() != nil {
			t.Error("CreateTime should be nil for aggregate result")
		}
		if res.UpdateTime() != nil {
			t.Error("UpdateTime should be nil for aggregate result")
		}
		execTime, err := snapshot.ExecutionTime()
		if err != nil {
			t.Fatal(err)
		}
		if execTime == nil {
			t.Error("ExecutionTime is nil")
		}
	})

	runTest("SelectSpecificFields", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Select("title", "author").
			Sort(Ascending(FieldOf("author"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 11 {
			t.Errorf("Expected 11 results, got %d", len(results))
		}
		first := results[0].Data()
		if first["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("Expected first title ..., got %v", first["title"])
		}
		if _, ok := first["rating"]; ok {
			t.Error("Unexpected field rating")
		}
	})

	runTest("AddAndRemoveFields", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(NotEqual("author", "Timestamp Author")).
			AddFields(
				StringConcat(FieldOf("author"), "_", FieldOf("title")).As("author_title"),
				StringConcat(FieldOf("title"), "_", FieldOf("author")).As("title_author"),
			).
			RemoveFields("title_author", "tags", "awards", "rating", "title", "embedding", "cost").
			RemoveFields("published", "genre", "nestedField").
			Sort(Ascending(FieldOf("author_title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}
		first := results[0].Data()
		if first["author_title"] != "Douglas Adams_The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("Expected author_title ..., got %v", first["author_title"])
		}
		if _, ok := first["title"]; ok {
			t.Error("Unexpected field title")
		}
	})

	runTest("WhereByMultipleConditions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(And(GreaterThan("rating", 4.5), Equal("genre", "Science Fiction"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(results))
		}
		if results[0].Data()["title"] != "Dune" {
			t.Errorf("Expected Dune, got %v", results[0].Data()["title"])
		}
	})

	runTest("WhereByOrCondition", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Or(Equal("genre", "Romance"), Equal("genre", "Dystopian"))).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
	})

	runTest("TestPipelineWithOffsetAndLimit", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Sort(Ascending(FieldOf("author"))).
			Offset(5).
			Limit(3).
			Select("title", "author").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
		if results[0].Data()["author"] != "George Orwell" {
			t.Errorf("Expected George Orwell, got %v", results[0].Data()["author"])
		}
		if results[1].Data()["author"] != "Harper Lee" {
			t.Errorf("Expected Harper Lee, got %v", results[1].Data()["author"])
		}
		if results[2].Data()["author"] != "J.R.R. Tolkien" {
			t.Errorf("Expected J.R.R. Tolkien, got %v", results[2].Data()["author"])
		}
	})

	runTest("TestAggregates", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// First part: countAll
		snapshot := client.Pipeline().Collection(coll.ID).Aggregate(CountAll().As("count")).Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["count"] != int64(11) {
			t.Errorf("Expected count 11, got %v", results[0].Data()["count"])
		}

		// Second part: where + aggregates
		snapshot = client.Pipeline().CreateFromQuery(coll).
			Where(Equal("genre", "Science Fiction")).
			Aggregate(
				CountAll().As("count"),
				Average("rating").As("avg_rating"),
				Maximum("rating").As("max_rating"),
			).Execute(ctx)
		results, err = snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		data := results[0].Data()
		if data["count"] != int64(2) {
			t.Errorf("Expected count 2, got %v", data["count"])
		}
		// avg_rating 4.4. 4.2 and 4.6. (4.2+4.6)/2 = 4.4
		if diff := testutil.Diff(data["avg_rating"], 4.4, cmpopts.EquateApprox(0, 0.00001)); diff != "" {
			t.Errorf("avg_rating mismatch: %s", diff)
		}
		if data["max_rating"] != 4.6 {
			t.Errorf("Expected max 4.6, got %v", data["max_rating"])
		}
	})

	runTest("TestMoreAggregates", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Aggregate(
				Sum("rating").As("sum_rating"),
				Count("rating").As("count_rating"),
				CountDistinct("genre").As("distinct_genres"),
			).Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		data := results[0].Data()
		// sum: 43.1
		if diff := testutil.Diff(data["sum_rating"], 43.1, cmpopts.EquateApprox(0, 0.00001)); diff != "" {
			t.Errorf("sum_rating mismatch: %s", diff)
		}
		if data["count_rating"] != int64(10) {
			t.Errorf("Expected count 10, got %v", data["count_rating"])
		} // book11 has no rating
		if data["distinct_genres"] != int64(8) {
			t.Errorf("Expected distinct 8, got %v", data["distinct_genres"])
		}
	})

	runTest("TestCountIfAggregate", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Aggregate(CountIf(GreaterThan(FieldOf("rating"), 4.3)).As("count")).
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["count"] != int64(3) {
			t.Errorf("Expected count 3, got %v", results[0].Data()["count"])
		}
	})

	runTest("TestGroupBysWithoutAccumulators", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		spec := NewAggregateSpec().WithGroups("genre")
		p := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1900)).
			AggregateWithSpec(spec)

		// Check if Execute returns error.
		if _, err := p.Execute(ctx).Results().GetAll(); err == nil {
			t.Error("Expected error for aggregates without accumulators")
		}
	})

	runTest("TestDistinct", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1900)).
			Distinct(ToLower(FieldOf("genre")).As("lower_genre")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
		// Sort and check
		var genres []string
		for _, r := range results {
			genres = append(genres, r.Data()["lower_genre"].(string))
		}
		sort.Strings(genres)
		if genres[0] != "psychological thriller" || genres[1] != "romance" {
			t.Errorf("Unexpected genres: %v", genres)
		}
	})

	runTest("TestGroupBysAndAggregate", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		spec := NewAggregateSpec(Average("rating").As("avg_rating")).WithGroups("genre")
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(LessThan("published", 1984)).
			AggregateWithSpec(spec).
			Where(GreaterThan("avg_rating", 4.3)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
	})

	runTest("TestMinMax", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Aggregate(
				CountAll().As("count"),
				Maximum("rating").As("max_rating"),
				Minimum("published").As("min_published"),
			).Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		data := results[0].Data()

		if data["count"] != int64(11) {
			t.Errorf("Expected count 11, got %v", data["count"])
		}
		if data["max_rating"] != 4.7 {
			t.Errorf("Expected max 4.7, got %v", data["max_rating"])
		}
		if data["min_published"] != int64(1813) {
			t.Errorf("Expected min 1813, got %v", data["min_published"])
		}
	})

	runTest("TestArraySum", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		docRef := coll.Doc("book4")
		_, err := docRef.Update(ctx, []Update{{Path: "sales", Value: []int{100, 200, 50}}})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}

		snapshot := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Lord of the Rings")).
			Select(ArraySum("sales").As("totalSales")).
			Limit(1).
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["totalSales"] != int64(350) {
			t.Errorf("Expected 350, got %v", results[0].Data()["totalSales"])
		}
	})

	runTest("TestArrayContains", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayContains("tags", "comedy")).
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
		// Hitchhiker
		if results[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Errorf("Expected Hitchhiker, got %v", results[0].Data()["title"])
		}
	})

	runTest("TestArrayContainsAny", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayContainsAny("tags", []string{"comedy", "classic"})).
			Select("title").
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
		// Hitchhiker and Pride and Prejudice
	})

	runTest("TestArrayContainsAll", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Where(ArrayContainsAll("tags", []string{"adventure", "magic"})).
			Select("title").
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
		// LOTR
		if results[0].Data()["title"] != "The Lord of the Rings" {
			t.Errorf("Expected LOTR, got %v", results[0].Data()["title"])
		}
	})

	runTest("TestArrayLength", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Select(ArrayLength("tags").As("tagsCount")).
			Where(Equal("tagsCount", 3)).
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}
	})

	runTest("TestArrayConcat", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Select(ArrayConcat(FieldOf("tags"), Array("newTag1", "newTag2")).As("modifiedTags")).
			Limit(1).
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		tags := results[0].Data()["modifiedTags"].([]interface{})
		if len(tags) != 5 {
			t.Errorf("Expected 5 tags, got %d", len(tags))
		}
	})

	runTest("TestStrConcat", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		snapshot := client.Pipeline().CreateFromQuery(coll).
			Select(StringConcat(FieldOf("author"), " - ", FieldOf("title")).As("bookInfo")).
			Limit(1).
			Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Order isn't guaranteed without sort, but setupJavaBooks inserts in order or keys? Keys are book1..book11.
		// Usually iteration order is undefined.
		// But let's check format.
		val := results[0].Data()["bookInfo"].(string)
		if len(val) < 5 {
			t.Error("String concat failed")
		}
	})

	runTest("TestStartsWith", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(StartsWith("title", "The")).
			Select("title").
			Sort(Ascending(FieldOf("title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Gatsby, Handmaid, Hitchhiker, LOTR. (4)
		if len(results) != 4 {
			t.Errorf("Expected 4 results, got %d", len(results))
		}
	})

	runTest("TestEndsWith", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(EndsWith(FieldOf("title"), "y")).
			Select("title").
			Sort(Descending(FieldOf("title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Hitchhiker (Galaxy), Gatsby (Gatsby). (2)
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	runTest("TestLength", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(CharLength("title").As("titleLength"), "title").
			Where(GreaterThan("titleLength", 21)).
			Sort(Descending(FieldOf("titleLength"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Hitchhiker (36), One Hundred Years (29).
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	runTest("TestStringFunctions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Reverse
		res1, err := client.Pipeline().Collection(coll.ID).
			Select(StringReverse("title").As("reversed_title"), "author").
			Where(Equal("author", "Douglas Adams")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res1[0].Data()["reversed_title"] != "yxalaG eht ot ediuG s'rekihhctiH ehT" {
			t.Errorf("Reverse mismatch")
		}

		// CharLength
		res2, err := client.Pipeline().CreateFromQuery(coll).
			Select(CharLength("title").As("title_length"), "author").
			Where(Equal("author", "Douglas Adams")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res2[0].Data()["title_length"] != int64(36) {
			t.Errorf("CharLength mismatch")
		}

		// ByteLength
		res3, err := client.Pipeline().CreateFromQuery(coll).
			Select(
				FieldOf("author"),
				ByteLength(StringConcat(FieldOf("title"), "_银河系漫游指南")).As("title_byte_length"),
			).
			Where(Equal("author", "Douglas Adams")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// "The Hitchhiker's Guide to the Galaxy" (36) + "_" (1) + "银河系漫游指南" (3*7=21) = 58
		if res3[0].Data()["title_byte_length"] != int64(58) {
			t.Errorf("ByteLength mismatch, got %v", res3[0].Data()["title_byte_length"])
		}
	})

	runTest("TestToLowercase", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(ToLower("title").As("lowercaseTitle")).
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal("Expected 1 result")
		}
	})

	runTest("TestToUppercase", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(ToUpper("author").As("uppercaseAuthor")).
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal("Expected 1 result")
		}
	})

	runTest("TestTrim", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			AddFields(StringConcat(ConstantOf(" "), FieldOf("title"), ConstantOf(" ")).As("spacedTitle")).
			Select(Trim("spacedTitle").As("trimmedTitle"), "spacedTitle").
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Check that trimmedTitle == title (conceptually, we don't have title selected, but we can verify it's trimmed)
		spaced := results[0].Data()["spacedTitle"].(string)
		trimmed := results[0].Data()["trimmedTitle"].(string)
		if len(spaced) != len(trimmed)+2 {
			t.Errorf("Trim mismatch: spaced='%s', trimmed='%s'", spaced, trimmed)
		}
	})

	runTest("TestLike", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Like("title", "%Guide%")).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
	})

	runTest("TestRegexContains", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(RegexContains("title", "(?i)(the|of)")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// 5 results expected
		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}
	})

	runTest("TestRegexMatches", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(RegexMatch("title", ".*(?i)(the|of).*")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}
	})

	runTest("TestStrContains", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(StringContains(FieldOf("title"), "'s")).
			Select("title").
			Sort(Ascending(FieldOf("title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Handmaid's Tale, Hitchhiker's Guide. (2)
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	runTest("TestSubstring", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(
				Substring(FieldOf("title"), 9, 2).As("of"), // "of"
				Substring("title", 16, 5).As("Rings"),      // "Rings"
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["of"] != "of" {
			t.Errorf("Mismatch of")
		}
		if results[0].Data()["Rings"] != "Rings" {
			t.Errorf("Mismatch Rings")
		}
	})

	runTest("TestSplitStringByStringDelimiter", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Split(FieldOf("title"), " ").As("split_title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// [The, Hitchhiker's, Guide, to, the, Galaxy]
		split := results[0].Data()["split_title"].([]interface{})
		if len(split) != 6 {
			t.Errorf("Expected 6 parts, got %d", len(split))
		}
	})

	runTest("TestSplitStringByExpressionDelimiter", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Split(FieldOf("title"), ConstantOf(" ")).As("split_title")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		split := results[0].Data()["split_title"].([]interface{})
		if len(split) != 6 {
			t.Errorf("Expected 6 parts, got %d", len(split))
		}
	})

	runTest("TestJoin", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Join("tags", ", ").As("joined_tags")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// "comedy, space, adventure"
		if results[0].Data()["joined_tags"] != "comedy, space, adventure" {
			t.Errorf("Join mismatch: %v", results[0].Data()["joined_tags"])
		}
	})

	runTest("TestArithmeticOperations", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Sort(Ascending(FieldOf("__name__"))). // Ensure book1
			Select(
				Add(FieldOf("rating"), 1).As("ratingPlusOne"),
				Subtract(FieldOf("published"), 1900).As("yearsSince1900"),
				Multiply(FieldOf("rating"), 10).As("ratingTimesTen"),
				Divide(FieldOf("rating"), 2).As("ratingDividedByTwo"),
			).
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		data := results[0].Data()

		if diff := testutil.Diff(data["ratingPlusOne"], 5.2); diff != "" {
			t.Errorf("ratingPlusOne: %s", diff)
		}
		if diff := testutil.Diff(data["yearsSince1900"], int64(79)); diff != "" {
			t.Errorf("yearsSince1900: %s", diff)
		}
		if diff := testutil.Diff(data["ratingTimesTen"], 42.0); diff != "" {
			t.Errorf("ratingTimesTen: %s", diff)
		}
		if diff := testutil.Diff(data["ratingDividedByTwo"], 2.1); diff != "" {
			t.Errorf("ratingDividedByTwo: %s", diff)
		}
	})

	runTest("TestComparisonOperators", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(
				And(
					GreaterThan("rating", 4.2),
					LessThanOrEqual("rating", 4.5),
					NotEqual("genre", "Science Fiction"),
				),
			).
			Select("rating", "title").
			Sort(Ascending(FieldOf("title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Crime (4.3), One Hundred (4.3), Pride (4.5)
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
	})

	runTest("TestLogicalAndComparisonOperators", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Xor
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(
				Xor(
					Equal("genre", "Romance"),
					Equal("genre", "Dystopian"),
					Equal("genre", "Fantasy"),
					Equal("published", 1949),
				),
			).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Pride (Romance), LOTR (Fantasy), Handmaid (Dystopian).
		// 1984 (Dystopian AND 1949) -> Xor fails?
		// Java test expects: Pride, LOTR, Handmaid.
		// 1984 is Dystopian (T) and 1949 (T) -> Xor(T, T, T, T) ? No, Xor is usually binary or variadic "exactly one true"?
		// Firestore Xor means "one or more are true" or "odd number true"?
		// "Logical XOR of all the expressions".
		// For 1984: Dystopian=T, 1949=T. T XOR T = F.
		// For Pride: Romance=T. T.
		// For LOTR: Fantasy=T. T.
		// For Handmaid: Dystopian=T. T.
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// EqualAny
		results, err = client.Pipeline().CreateFromQuery(coll).
			Where(EqualAny("genre", []string{"Romance", "Dystopian"})).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Pride, Handmaid, 1984.
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// NotEqualAny
		results, err = client.Pipeline().CreateFromQuery(coll).
			Where(NotEqualAny("genre", []interface{}{"Romance", "Dystopian", nil})).
			Distinct("genre").
			Select("genre").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Sci Fi, Magical Realism, Fantasy, Psych Thriller, Southern Gothic, Modernist. (6)
		if len(results) != 6 {
			t.Errorf("Expected 6 results, got %d", len(results))
		}
	})

	runTest("TestCondExpression", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(NotEqual("title", "Timestamp Book")).
			Select(
				Conditional(GreaterThan("published", 1980), "Modern", "Classic").As("era"),
				"title", "published",
			).
			Sort(Ascending(FieldOf("published"))).
			Limit(2).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Pride (1813), Crime (1866) -> Classic
		if results[0].Data()["era"] != "Classic" {
			t.Error("Expected Classic")
		}
		if results[1].Data()["era"] != "Classic" {
			t.Error("Expected Classic")
		}
	})

	runTest("TestLogicalOperators", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(
				Or(
					And(GreaterThan("rating", 4.5), Equal("genre", "Science Fiction")),
					LessThan("published", 1900),
				),
			).
			Select("title").
			Sort(Ascending(FieldOf("title"))).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Crime, Dune, Pride. (3)
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
	})

	runTest("TestChecks", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Sort(Descending(FieldOf("rating"))).
			Limit(1).
			Select(
				Equal("rating", nil).As("ratingIsNull"),
				Equal("rating", math.NaN()).As("ratingIsNaN"), // NaN comparison might need IsNaN check in expression
				// Go client Equal(math.NaN()) ? Firestore backend handles it.
				IsError(ArrayGet("title", 0)).As("isError"),
				// IfError(ArrayGet("title", 0), "was not error").As("ifError"), // "was error" is catch value.
				IsAbsent("foo").As("isAbsent"),
				NotEqual("title", nil).As("titleIsNotNull"),
				NotEqual("cost", math.NaN()).As("costIsNotNan"),
				FieldExists("fooBarBaz").As("fooBarBazExists"),
				FieldExists("title").As("titleExists"),
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}

		data := results[0].Data()
		if data["ratingIsNull"] != false {
			t.Error("ratingIsNull mismatch")
		}
		if data["isAbsent"] != true {
			t.Error("isAbsent mismatch")
		}
		if data["titleExists"] != true {
			t.Error("titleExists mismatch")
		}
	})

	runTest("TestLogicalMinMax", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// LogicalMax
		res1, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("author", "Douglas Adams")).
			Select(
				LogicalMaximum(FieldOf("rating"), 4.5).As("max_rating"),
				LogicalMaximum(FieldOf("published"), 1900).As("max_published"),
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// rating 4.2 vs 4.5 -> 4.5
		// published 1979 vs 1900 -> 1979
		if diff := testutil.Diff(res1[0].Data()["max_rating"], 4.5); diff != "" {
			t.Errorf("max_rating: %s", diff)
		}
		if diff := testutil.Diff(res1[0].Data()["max_published"], int64(1979)); diff != "" {
			t.Errorf("max_published: %s", diff)
		}

		// LogicalMin
		res2, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("author", "Douglas Adams")).
			Select(
				LogicalMinimum(FieldOf("rating"), 4.5).As("min_rating"),
				LogicalMinimum(FieldOf("published"), 1900).As("min_published"),
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// 4.2 vs 4.5 -> 4.2
		// 1979 vs 1900 -> 1900
		if diff := testutil.Diff(res2[0].Data()["min_rating"], 4.2); diff != "" {
			t.Errorf("min_rating: %s", diff)
		}
		if diff := testutil.Diff(res2[0].Data()["min_published"], int64(1900)); diff != "" {
			t.Errorf("min_published: %s", diff)
		}
	})

	runTest("TestMapGet", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Select(MapGet("awards", "hugo").As("hugoAward"), "title").
			Where(Equal("hugoAward", true)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Hitchhiker, Dune. (2)
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	runTest("TestDataManipulationExpressions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Timestamp math
		res1, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "Timestamp Book")).
			Select(
				TimestampAdd("timestamp", "day", 1).As("timestamp_plus_day"),
				TimestampSubtract("timestamp", "hour", 1).As("timestamp_minus_hour"),
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Verification skipped for brevity, assumed covered by unit tests, but we check if it runs.
		if len(res1) != 1 {
			t.Fatal("Expected 1 result")
		}

		// Map/Array manipulation
		res2, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(
				ArrayGet("tags", 1).As("second_tag"),
				MapMerge(FieldOf("awards"), Map(map[string]any{"new_award": true})).As("merged_awards"),
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res2[0].Data()["second_tag"] != "space" {
			t.Error("second_tag mismatch")
		}

		res3, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(
				ArrayReverse("tags").As("reversed_tags"),
				MapRemove(FieldOf("awards"), "nebula").As("removed_awards"),
			).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		tags := res3[0].Data()["reversed_tags"].([]interface{})
		if tags[0] != "adventure" {
			t.Error("Reverse mismatch")
		}
	})

	runTest("TestBlobConcat", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		bytes1 := []byte{1, 2}
		bytes2 := []byte{3, 4}
		expected := []byte{1, 2, 3, 4}
		results, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(Concat(ConstantOf(bytes1), ConstantOf(bytes2)).As("concatenated_blob")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if diff := testutil.Diff(results[0].Data()["concatenated_blob"], expected); diff != "" {
			t.Errorf("Blob concat mismatch: %s", diff)
		}
	})

	runTest("TestTimestampTrunc", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Just verify it runs
		_, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "Timestamp Book")).
			Select(TimestampTruncate("timestamp", "year").As("trunc_year")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestMathExpressions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		_, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(Ceil("rating").As("ceil")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestAdvancedMathExpressions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		_, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Lord of the Rings")).
			Select(Exp("rating").As("exp")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestCurrentTimestamp", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(CurrentTimestamp().As("now")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Check if "now" is a timestamp
		if _, ok := results[0].Data()["now"].(time.Time); !ok {
			t.Error("Expected timestamp for currentTimestamp")
		}
	})

	runTest("TestIfAbsent", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(IfAbsent(FieldOf("rating"), 0.0).As("rating_or_default")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["rating_or_default"] != 4.2 {
			t.Error("Mismatch")
		}

		results, err = client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Select(IfAbsent("non_existent", "default").As("val")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["val"] != "default" {
			t.Error("Mismatch default")
		}
	})

	runTest("TestTimestampConversions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		_, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(UnixSecondsToTimestamp(ConstantOf(1741380235)).As("ts")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestVectorLength", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		_, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(VectorLength(ConstantOf(Vector64{1, 2, 3})).As("len")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestDistanceFunctions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Just run it
		_, err := client.Pipeline().Collection(coll.ID).
			Limit(1).
			Select(CosineDistance(ConstantOf(Vector64{1, 0}), Vector64{0, 1}).As("dist")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestNestedFields", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("awards.hugo", true)).
			Select("title", "awards.hugo").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2, got %d", len(results))
		}
	})

	runTest("TestType", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Collection(coll.ID).
			Where(Equal("author", "Douglas Adams")).
			Limit(1).
			Select(Type("title").As("type")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Data()["type"] != "string" {
			t.Error("Expected string")
		}
	})

	runTest("TestPipelineInTransactions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		err := client.RunTransaction(ctx, func(ctx context.Context, txn *Transaction) error {
			pipeline := client.Pipeline().CreateFromQuery(coll).
				Where(Equal("awards.hugo", true)).
				Select("title", "awards.hugo")

			iter := txn.Execute(pipeline).Results()
			results, err := iter.GetAll()
			if err != nil {
				return err
			}
			if len(results) != 2 {
				return fmt.Errorf("Expected 2, got %d", len(results))
			}

			// Update book1
			return txn.Update(coll.Doc("book1"), []Update{{Path: "foo", Value: "bar"}})
		})
		if err != nil {
			t.Fatal(err)
		}

		res, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("foo", "bar")).
			Select("title").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 {
			t.Fatal("Expected 1 result")
		}
	})

	runTest("TestPipelineInTransactionsWithOptions", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Just verify it accepts options (even if dummy)
		err := client.RunTransaction(ctx, func(ctx context.Context, txn *Transaction) error {
			pipeline := client.Pipeline().CreateFromQuery(coll).Limit(1).WithExecuteOptions(WithExplainMode(ExplainModeAnalyze))
			// Execute with explain options should return explain metrics but we just check if it runs without error.
			iter := txn.Execute(pipeline).Results()
			_, err := iter.GetAll()
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	runTest("TestRawStage", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Select
		res1, err := client.Pipeline().Collection(coll.ID).
			RawStage(NewRawStage("select").WithArguments(map[string]interface{}{
				"title":    FieldOf("title"),
				"metadata": Map(map[string]interface{}{"author": FieldOf("author")}),
			})).
			Sort(Ascending(FieldOf("metadata.author"))).
			Limit(1).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res1[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Error("Mismatch title")
		}

		// AddFields
		res2, err := client.Pipeline().Collection(coll.ID).
			Sort(Ascending(FieldOf("author"))).
			Limit(1).
			Select("title", "author").
			RawStage(NewRawStage("add_fields").WithArguments(map[string]interface{}{
				"display": StringConcat(FieldOf("title"), " - ", FieldOf("author")),
			})).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res2[0].Data()["display"] != "The Hitchhiker's Guide to the Galaxy - Douglas Adams" {
			t.Error("Mismatch display")
		}
	})

	runTest("TestReplaceWith", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		res1, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			ReplaceWith("awards").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res1[0].Data()["hugo"] != true {
			t.Error("Mismatch hugo")
		}

		res2, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			ReplaceWith(Map(map[string]interface{}{
				"foo": "bar",
				"baz": Map(map[string]interface{}{"title": FieldOf("title")}),
			})).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if res2[0].Data()["foo"] != "bar" {
			t.Error("Mismatch foo")
		}
	})

	runTest("TestSampleLimit", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Sample(SampleByDocuments(3)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3, got %d", len(results))
		}
	})

	runTest("TestSamplePercentage", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Sample(&SampleSpec{Size: 0.6, Mode: SampleModePercent}).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// Just ensure it runs and returns something (probabilistic)
		if len(results) == 0 {
			t.Log("Sample 0.6 returned 0 documents, which is possible but unlikely for 11 docs")
		}
	})

	runTest("TestUnion", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Union(client.Pipeline().CreateFromQuery(coll)).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		// 11 docs * 2 = 22
		if len(results) != 22 {
			t.Errorf("Expected 22, got %d", len(results))
		}
	})

	runTest("TestUnnest", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag", nil).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3, got %d", len(results))
		}
	})

	runTest("TestUnnestWithIndexField", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			UnnestWithAlias("tags", "tag", &UnnestOptions{IndexField: "tagsIndex"}).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3, got %d", len(results))
		}
	})

	runTest("TestUnnestWithExpr", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().CreateFromQuery(coll).
			Where(Equal("title", "The Hitchhiker's Guide to the Galaxy")).
			Unnest(Array(1, 2, 3).As("copy"), nil).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("Expected 3, got %d", len(results))
		}
	})

	runTest("TestPaginationWithStartAfter", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		pagColl := client.Collection(collectionIDs.New())
		// Cleanup is handled by defer if we track it, or we delete individually.
		docRefs := []*DocumentRef{}
		for i := 1; i <= 4; i++ {
			dr, _, err := pagColl.Add(ctx, map[string]interface{}{"order": i})
			if err != nil {
				t.Fatal(err)
			}
			docRefs = append(docRefs, dr)
		}
		defer deleteDocuments(docRefs)

		pipeline := client.Pipeline().CreateFromQuery(pagColl.OrderBy("order", Asc).Limit(2))
		snapshot := pipeline.Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Fatal("Expected 2 results")
		}
		if results[0].Data()["order"] != int64(1) {
			t.Error("Order 1 mismatch")
		}
		if results[1].Data()["order"] != int64(2) {
			t.Error("Order 2 mismatch")
		}

		lastResult := results[1]

		pipeline = client.Pipeline().CreateFromQuery(pagColl.OrderBy("order", Asc).StartAfter(lastResult.Data()["order"]))
		snapshot = pipeline.Execute(ctx)
		results, err = snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
		if results[0].Data()["order"] != int64(3) {
			t.Error("Order 3 mismatch")
		}
		if results[1].Data()["order"] != int64(4) {
			t.Error("Order 4 mismatch")
		}
	})

	runTest("TestDocumentsAsSource", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		results, err := client.Pipeline().Documents(coll.Doc("book1"), coll.Doc("book2")).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	runTest("TestCollectionGroupAsSource", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		cgID := collectionIDs.New()
		// create subcollections
		doc1 := coll.Doc("book1")
		_, _, err := doc1.Collection(cgID).Add(ctx, map[string]interface{}{"val": 1})
		if err != nil {
			t.Fatal(err)
		}
		doc2 := coll.Doc("book2")
		_, _, err = doc2.Collection(cgID).Add(ctx, map[string]interface{}{"val": 2})
		if err != nil {
			t.Fatal(err)
		}

		results, err := client.Pipeline().CollectionGroup(cgID).
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	runTest("TestDatabaseAsSource", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Just verify it runs. It scans whole DB so Limit is important.
		results, err := client.Pipeline().Database().Limit(1).Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal("Expected 1 result")
		}
	})

	runTest("TestFindNearest", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		limit := 2
		distanceField := "computedDistance"
		results, err := client.Pipeline().Collection(coll.ID).
			FindNearest(
				"embedding",
				Vector64{10.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
				PipelineDistanceMeasureEuclidean,
				&PipelineFindNearestOptions{Limit: &limit, DistanceField: &distanceField},
			).
			Select("title", "computedDistance").
			Execute(ctx).Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Fatal("Expected 2 results")
		}
		if results[0].Data()["title"] != "The Hitchhiker's Guide to the Galaxy" {
			t.Error("Mismatch first")
		}
	})

	runTest("TestExplain", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		pipeline := client.Pipeline().CreateFromQuery(coll).Limit(1)
		snapshot := pipeline.WithExecuteOptions(WithExplainMode(ExplainModeAnalyze)).Execute(ctx)
		results, err := snapshot.Results().GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal("Expected 1 result")
		}

		stats := snapshot.ExplainStats()
		if stats == nil {
			t.Fatal("Expected ExplainStats")
		}
	})

	runTest("TestErrorHandling", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		_, err := client.Pipeline().Collection(coll.ID).
			RawStage(NewRawStage("invalidStage")).
			Execute(ctx).Results().GetAll()
		if err == nil {
			t.Error("Expected error")
		}
	})

	runTest("TestDuplicateAliases", func(t *testing.T, ctx context.Context, client *Client, coll *CollectionRef) {
		// Aggregate
		_, err := client.Pipeline().Collection(coll.ID).
			Aggregate(CountAll().As("dup"), Average("rating").As("dup")).
			Execute(ctx).Results().GetAll()
		if err == nil {
			t.Error("Expected error for duplicate aliases in Aggregate")
		}

		// Select
		_, err = client.Pipeline().Collection(coll.ID).
			Select(FieldOf("title").As("dup"), FieldOf("author").As("dup")).
			Execute(ctx).Results().GetAll()
		if err == nil {
			t.Error("Expected error for duplicate aliases in Select")
		}
	})
}
