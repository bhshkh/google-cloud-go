package firestore

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"google.golang.org/api/iterator"
)

func TestIntegration_PipelineStages(t *testing.T) {
	if testParams[firestoreEditionKey].(firestoreEdition) != editionEnterprise {
		t.Skip("Skipping pipeline queries tests since the firestore edition of", testParams[databaseIDKey].(string), "database is not enterprise")
	}
	ctx := context.Background()
	client := integrationClient(t)
	coll := integrationColl(t)
	h := testHelper{t}
	type Author struct {
		Name    string `firestore:"name"`
		Country string `firestore:"country"`
	}
	type Book struct {
		Title     string `firestore:"title"`
		Author    `firestore:"author"`
		Genre     string   `firestore:"genre"`
		Published int      `firestore:"published"`
		Rating    float64  `firestore:"rating"`
		Tags      []string `firestore:"tags"`
	}
	books := []Book{
		{
			Title:     "The Hitchhiker's Guide to the Galaxy",
			Author:    Author{Name: "Douglas Adams", Country: "UK"},
			Genre:     "Science Fiction",
			Published: 1979,
			Rating:    4.2,
			Tags:      []string{"comedy", "space", "adventure"},
		},
		{
			Title:     "Pride and Prejudice",
			Author:    Author{Name: "Jane Austen", Country: "UK"},
			Genre:     "Romance",
			Published: 1813,
			Rating:    4.5,
			Tags:      []string{"classic", "social commentary", "love"},
		},
		{
			Title:     "One Hundred Years of Solitude",
			Author:    Author{Name: "Gabriel García Márquez", Country: "Colombia"},
			Genre:     "Magical Realism",
			Published: 1967,
			Rating:    4.3,
			Tags:      []string{"family", "history", "fantasy"},
		},
		{
			Title:     "The Lord of the Rings",
			Author:    Author{Name: "J.R.R. Tolkien", Country: "UK"},
			Genre:     "Fantasy",
			Published: 1954,
			Rating:    4.7,
			Tags:      []string{"adventure", "magic", "epic"},
		},
		{
			Title:     "The Handmaid's Tale",
			Author:    Author{Name: "Margaret Atwood", Country: "Canada"},
			Genre:     "Dystopian",
			Published: 1985,
			Rating:    4.1,
			Tags:      []string{"feminism", "totalitarianism", "resistance"},
		},
		{
			Title:     "Crime and Punishment",
			Author:    Author{Name: "Fyodor Dostoevsky", Country: "Russia"},
			Genre:     "Psychological Thriller",
			Published: 1866,
			Rating:    4.3,
			Tags:      []string{"philosophy", "crime", "redemption"},
		},
		{
			Title:     "To Kill a Mockingbird",
			Author:    Author{Name: "Harper Lee", Country: "USA"},
			Genre:     "Southern Gothic",
			Published: 1960,
			Rating:    4.2,
			Tags:      []string{"racism", "injustice", "coming-of-age"},
		},
		{
			Title:     "1984",
			Author:    Author{Name: "George Orwell", Country: "UK"},
			Genre:     "Dystopian",
			Published: 1949,
			Rating:    4.2,
			Tags:      []string{"surveillance", "totalitarianism", "propaganda"},
		},
		{
			Title:     "The Great Gatsby",
			Author:    Author{Name: "F. Scott Fitzgerald", Country: "USA"},
			Genre:     "Modernist",
			Published: 1925,
			Rating:    4.0,
			Tags:      []string{"wealth", "american dream", "love"},
		},
		{
			Title:     "Dune",
			Author:    Author{Name: "Frank Herbert", Country: "USA"},
			Genre:     "Science Fiction",
			Published: 1965,
			Rating:    4.6,
			Tags:      []string{"politics", "desert", "ecology"},
		},
	}
	timeBeforeCreate := time.Now().Add(-time.Minute)
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
		iter := client.Pipeline().Collection(coll.ID).AddFields(Multiply(FieldOf("rating"), 2).As("doubled_rating")).Limit(1).Execute(ctx)
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
		iter := client.Pipeline().Collection(coll.ID).Aggregate(Count("rating").As("total_books")).Execute(ctx)
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
		iter := client.Pipeline().Collection(coll.ID).AggregateWithSpec(spec).Execute(ctx)
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
		iter := client.Pipeline().Collection(coll.ID).Distinct("genre").Execute(ctx)
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
		iter := client.Pipeline().Documents(docRefs[0], docRefs[1]).Execute(ctx)
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
		cgParentColl := client.Collection(collectionIDs.New())
		cgCollID := collectionIDs.New()
		doc1 := cgParentColl.Doc("cg_doc1")
		doc2 := cgParentColl.Doc("cg_doc2")
		cgColl1 := doc1.Collection(cgCollID)
		cgColl2 := doc2.Collection(cgCollID)
		cgDoc1 := cgColl1.NewDoc()
		cgDoc2 := cgColl2.NewDoc()
		h.mustCreate(cgDoc1, map[string]string{"val": "a"})
		h.mustCreate(cgDoc2, map[string]string{"val": "b"})
		t.Cleanup(func() {
			deleteDocuments([]*DocumentRef{cgDoc1, cgDoc2, doc1, doc2})
		})
		iter := client.Pipeline().CollectionGroup(cgCollID, nil).Execute(ctx)
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
		iter := client.Pipeline().Database().Limit(2).Execute(ctx)
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
		vectorColl := client.Collection(collectionIDs.New())
		for _, d := range docsWithVector {
			docRef := vectorColl.NewDoc()
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
		iter := client.Pipeline().Collection(vectorColl.ID).
			FindNearest("vector", queryVector, PipelineDistanceMeasureEuclidean, options).
			Execute(ctx)
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
		if err != nil {
			t.Fatalf("Failed to get data: %v", err)
		}
		if dist1[distanceField].(float64) > dist2[distanceField].(float64) {
			t.Errorf("documents are not sorted by distance")
		}
		// Check if the correct documents are returned
		if dist1["id"] != "doc1" {
			t.Errorf("got doc id %q, want 'doc1'", dist1["id"])
		}
	})
	t.Run("Limit", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Limit(3).Execute(ctx)
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
		iter := client.Pipeline().Collection(coll.ID).Sort(Ascending(FieldOf("published"))).Offset(2).Limit(1).Execute(ctx)
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
		iter := client.Pipeline().Collection(coll.ID).Raw(NewRawStage("limit").WithArguments(3)).Execute(ctx)
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("got %d documents, want 3", len(results))
		}

		// Using RawStage to perform a Select operation with options
		iter = client.Pipeline().Collection(coll.ID).Raw(NewRawStage("select").WithArguments(map[string]interface{}{"title": FieldOf("title")})).Limit(1).Execute(ctx)
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
			Execute(ctx)
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
		replaceColl := client.Collection(collectionIDs.New())
		docRef := replaceColl.NewDoc()
		h.mustCreate(docRef, docWithMap)
		t.Cleanup(func() {
			deleteDocuments([]*DocumentRef{docRef})
		})
		iter := client.Pipeline().Collection(replaceColl.ID).
			Where(Equal(FieldOf("id"), "docWithMap")).
			Replace("data").
			Execute(ctx)
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
			iter := client.Pipeline().Collection(coll.ID).Sample(SampleByDocuments(5)).Execute(ctx)
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
			iter := client.Pipeline().Collection(coll.ID).Sample(&SampleSpec{Size: 0.6, Mode: SampleModePercent}).Execute(ctx)
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
		iter := client.Pipeline().Collection(coll.ID).Select("title", "author.name").Limit(1).Execute(ctx)
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
		if _, ok := data["genre"]; ok {
			t.Error("unexpected 'genre' field")
		}
	})
	t.Run("Sort", func(t *testing.T) {
		iter := client.Pipeline().Collection(coll.ID).Sort(Descending(FieldOf("rating"))).Limit(1).Execute(ctx)
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
		iter := employeePipeline.Union(customerPipeline).Execute(context.Background())
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
			Execute(ctx)
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
			Execute(ctx)
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
			if err != nil {
				t.Fatalf("Failed to get data: %v", err)
			}
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
		iter := client.Pipeline().Collection(coll.ID).Where(Equal(FieldOf("author.country"), "UK")).Execute(ctx)
		defer iter.Stop()
		results, err := iter.GetAll()
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		if len(results) != 4 {
			t.Errorf("got %d documents, want 4", len(results))
		}
	})
	t.Run("WithReadOptions", func(t *testing.T) {
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

		iter := client.Pipeline().Collection(coll.ID).WithReadOptions(ReadTime(timeBeforeCreate)).Execute(ctx)
		res, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 0 {
			t.Errorf("got %d documents, want 0", len(res))
		}
	})
	t.Run("WithTransaction", func(t *testing.T) {
		p := client.Pipeline().Collection(coll.ID)
		err := client.RunTransaction(ctx, func(ctx context.Context, txn *Transaction) error {
			iter := txn.Execute(p)
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

	t.Run("CollectionWithOptions", func(t *testing.T) {
		hints := CollectionHints{}.WithForceIndex("title")
		iter := client.Pipeline().Collection(coll.ID, WithCollectionHints(hints)).Execute(ctx)
		defer iter.Stop()
		_, err := iter.Next()
		if err == nil {
			t.Errorf("Expected error due to non-existent index, but got nil")
		}
	})

	t.Run("WithExecutionOptionsAnalyze", func(t *testing.T) {
		p := client.Pipeline().Collection(coll.ID)
		err := client.RunTransaction(ctx, func(ctx context.Context, txn *Transaction) error {
			iter := txn.Execute(p, WithIndexMode("recommended"), WithExplainMode(ExecutionModeAnalyze))
			_, err := iter.GetAll()
			if err != nil {
				t.Fatal("GetAll failed:", err)
			}

			if iter.ExplainStats() == nil {
				t.Fatal("ExplainStats got nil, want non-nil")
			}
			rawData, err := iter.ExplainStats().GetRawData()
			if err != nil {
				t.Fatal("GetRawData failed:", err)
			}
			if rawData == nil {
				t.Fatal("GetRawData got nil, want non-nil")
			}
			stringData, err := iter.ExplainStats().GetText()
			if err != nil {
				t.Fatal("GetText failed:", err)
			}
			if stringData == "" {
				t.Fatal("GetText got nil, want non-nil")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}
