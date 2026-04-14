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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIntegration_PipelineSubqueries(t *testing.T) {
	skipIfEdition(t, "Pipeline queries", editionStandard)

	ctx := context.Background()
	client := integrationClient(t)
	h := testHelper{t}

	coll := client.Collection(collectionIDs.New())
	docRefs := []*DocumentRef{}
	for k, v := range bookDocs {
		doc := coll.Doc(k)
		docRefs = append(docRefs, doc)
		h.mustCreate(doc, v)
	}
	t.Cleanup(func() { deleteDocuments(docRefs) })

	t.Run("testZeroResultScalarReturnsNull", func(t *testing.T) {
		doc := coll.Doc("book1")
		emptyScalar := client.Pipeline().Collection(doc.Collection("reviews").ID).
			Where(Equal("reviewer", "Alice")).
			Select(Fields(CurrentDocument().As("data")))

		iter := client.Pipeline().Collection(coll.ID).
			Select(Fields(emptyScalar.ToScalarExpression().As("first_review_data"))).
			Limit(1).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"first_review_data": nil},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testArraySubqueryJoinAndEmptyResult", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "The Hitchhiker's Guide to the Galaxy", "reviewer": "Alice"})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "The Hitchhiker's Guide to the Galaxy", "reviewer": "Bob"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Select(Fields(FieldOf("reviewer").As("reviewer"))).
			Sort(Orders(FieldOf("reviewer").Ascending()))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Or(
				Equal("title", "The Hitchhiker's Guide to the Galaxy"),
				Equal("title", "Pride and Prejudice"),
			)).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews_data"))).
			Select(Fields("title", "reviews_data")).
			Sort(Orders(FieldOf("title").Descending())).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "The Hitchhiker's Guide to the Galaxy", "reviews_data": []any{"Alice", "Bob"}},
			{"title": "Pride and Prejudice", "reviews_data": []any{}},
		}
		isEqualTo(t, docs, want)
	})

	t.Run("testMultipleArraySubqueriesOnBooks", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())
		authorsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "1984", "rating": 5})
		a1 := authorsColl.Doc("a1")
		h.mustCreate(a1, map[string]interface{}{"authorName": "George Orwell", "nationality": "British"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, a1}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Select(Fields(FieldOf("rating").As("rating")))

		authorsSub := client.Pipeline().Collection(authorsColl.ID).
			Where(Equal("authorName", Variable("author_name"))).
			Select(Fields(FieldOf("nationality").As("nationality")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"), FieldOf("author").As("author_name"))).
			AddFields(Selectables(
				reviewsSub.ToArrayExpression().As("reviews_data"),
				authorsSub.ToArrayExpression().As("authors_data"),
			)).
			Select(Fields("title", "reviews_data", "authors_data")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "reviews_data": []any{int64(5)}, "authors_data": []any{"British"}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testArraySubqueryJoinMultipleFieldsPreservesMap", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "1984", "reviewer": "Alice", "rating": 5})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "1984", "reviewer": "Bob", "rating": 4})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Select(Fields(FieldOf("reviewer").As("reviewer"), FieldOf("rating").As("rating"))).
			Sort(Orders(FieldOf("reviewer").Ascending()))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews_data"))).
			Select(Fields("title", "reviews_data")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{
				"title": "1984",
				"reviews_data": []any{
					map[string]any{"reviewer": "Alice", "rating": int64(5)},
					map[string]any{"reviewer": "Bob", "rating": int64(4)},
				},
			},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testArraySubqueryInWhereStageOnBooks", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "Dune", "reviewer": "Paul"})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "Foundation", "reviewer": "Hari"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Select(Fields(FieldOf("reviewer").As("reviewer")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Or(Equal("title", "Dune"), Equal("title", "The Great Gatsby"))).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			Where(ArrayContains(reviewsSub.ToArrayExpression(), "Paul")).
			Select(Fields("title")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "Dune"},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testScalarSubquerySingleAggregationUnwrapping", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "1984", "rating": 4})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "1984", "rating": 5})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Aggregate(Accumulators(Average("rating").As("val")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(reviewsSub.ToScalarExpression().As("average_rating"))).
			Select(Fields("title", "average_rating")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "average_rating": 4.5},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testScalarSubqueryMultipleAggregationsMapWrapping", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "1984", "rating": 4})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "1984", "rating": 5})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Aggregate(Accumulators(Average("rating").As("avg"), CountAll().As("count")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(reviewsSub.ToScalarExpression().As("stats"))).
			Select(Fields("title", "stats")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "stats": map[string]any{"avg": 4.5, "count": int64(2)}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testScalarSubqueryZeroResults", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Aggregate(Accumulators(Average("rating").As("avg")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(reviewsSub.ToScalarExpression().As("average_rating"))).
			Select(Fields("title", "average_rating")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "average_rating": nil},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testScalarSubqueryMultipleResultsRuntimeError", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "1984", "rating": 4})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "1984", "rating": 5})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(reviewsSub.ToScalarExpression().As("review_data"))).
			Execute(ctx).Results()
		defer iter.Stop()

		_, err := iter.GetAll()
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		status, ok := status.FromError(err)
		if !ok || status.Code() != codes.InvalidArgument || !strings.Contains(status.Message(), "Subpipeline returned multiple results") {
			t.Fatalf("expected status error, got %v", err)
		}
	})

	t.Run("testMixedScalarAndArraySubqueries", func(t *testing.T) {
		reviewsColl := client.Collection(collectionIDs.New())

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookTitle": "1984", "reviewer": "Alice", "rating": 4})
		r2 := reviewsColl.Doc("r2")
		h.mustCreate(r2, map[string]interface{}{"bookTitle": "1984", "reviewer": "Bob", "rating": 5})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{r1, r2}) })

		arraySub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Select(Fields(FieldOf("reviewer").As("reviewer"))).
			Sort(Orders(FieldOf("reviewer").Ascending()))

		scalarSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookTitle", Variable("book_title"))).
			Aggregate(Accumulators(Average("rating").As("val")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("title").As("book_title"))).
			AddFields(Selectables(
				arraySub.ToArrayExpression().As("all_reviewers"),
				scalarSub.ToScalarExpression().As("average_rating"),
			)).
			Select(Fields("title", "all_reviewers", "average_rating")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{
				"title":          "1984",
				"all_reviewers":  []any{"Alice", "Bob"},
				"average_rating": 4.5,
			},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testSingleScopeVariableUsage", func(t *testing.T) {
		coll := client.Collection(collectionIDs.New())

		doc1 := coll.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"price": 100})
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1}) })

		iter := client.Pipeline().Collection(coll.ID).
			Define(AliasedExpressions(FieldOf("price").Multiply(0.8).As("discount"))).
			Where(Variable("discount").LessThan(50.0)).
			Select(Fields("price")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		containsExactly(t, docs, []map[string]any{})

		doc2 := coll.Doc("doc2")
		h.mustCreate(doc2, map[string]interface{}{"price": 50})
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc2}) })

		iter2 := client.Pipeline().Collection(coll.ID).
			Define(AliasedExpressions(FieldOf("price").Multiply(0.8).As("discount"))).
			Where(Variable("discount").LessThan(50.0)).
			Select(Fields("price")).
			Execute(ctx).Results()
		defer iter2.Stop()

		docs2, err := iter2.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}

		want := []map[string]any{
			{"price": int64(50)},
		}
		containsExactly(t, docs2, want)
	})

	t.Run("testExplicitFieldBindingScopeBridging", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		doc1 := outerColl.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"title": "1984", "id": "1"})

		reviewsColl := client.Collection(collectionIDs.New())
		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookId": "1", "reviewer": "Alice"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, r1}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookId", Variable("rid"))).
			Select(Fields(FieldOf("reviewer").As("reviewer")))

		iter := client.Pipeline().Collection(outerColl.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("id").As("rid"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews"))).
			Select(Fields("title", "reviews")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "reviews": []any{"Alice"}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testMultipleVariableBindings", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		reviewsColl := client.Collection(collectionIDs.New())

		doc1 := outerColl.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"title": "1984", "id": "1", "category": "sci-fi"})

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookId": "1", "category": "sci-fi", "reviewer": "Alice"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, r1}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(And(
				Equal("bookId", Variable("rid")),
				Equal("category", Variable("rcat")),
			)).
			Select(Fields(FieldOf("reviewer").As("reviewer")))

		iter := client.Pipeline().Collection(outerColl.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(FieldOf("id").As("rid"), FieldOf("category").As("rcat"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews"))).
			Select(Fields("title", "reviews")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "reviews": []any{"Alice"}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testCurrentDocumentBinding", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		reviewsColl := client.Collection(collectionIDs.New())

		doc1 := outerColl.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"title": "1984", "author": "George Orwell"})

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"authorName": "George Orwell", "reviewer": "Alice"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, r1}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("authorName", Variable("doc").GetField("author"))).
			Select(Fields(FieldOf("reviewer").As("reviewer")))

		iter := client.Pipeline().Collection(outerColl.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(CurrentDocument().As("doc"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews"))).
			Select(Fields("title", "reviews")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "reviews": []any{"Alice"}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testUnboundVariableCornerCase", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		iter := client.Pipeline().Collection(outerColl.ID).
			Where(Equal("title", Variable("unknown_var"))).
			Execute(ctx).Results()
		defer iter.Stop()

		_, err := iter.GetAll()
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		s, ok := status.FromError(err)
		if !ok {
			t.Fatalf("Failed to convert error to gRPC status: %v", err)
		}
		if s.Code() != codes.InvalidArgument || !strings.Contains(s.Message(), "The variable 'unknown_var' was either never defined") {
			t.Fatalf("expected error, got %v", err)
		}
	})

	t.Run("testVariableShadowingCollision", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		innerColl := client.Collection(collectionIDs.New())

		doc1 := outerColl.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"title": "1984"})
		i1 := innerColl.Doc("i1")
		h.mustCreate(i1, map[string]interface{}{"id": "test"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, i1}) })

		sub := client.Pipeline().Collection(innerColl.ID).
			Define(AliasedExpressions(ConstantOf("inner_val").As("x"))).
			Select(Fields(Variable("x").As("val")))

		iter := client.Pipeline().Collection(outerColl.ID).
			Where(Equal("title", "1984")).
			Limit(1).
			Define(AliasedExpressions(ConstantOf("outer_val").As("x"))).
			AddFields(Selectables(sub.ToArrayExpression().As("shadowed"))).
			Select(Fields("shadowed")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"shadowed": []any{"inner_val"}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testMissingFieldOnCurrentDocument", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		reviewsColl := client.Collection(collectionIDs.New())

		doc1 := outerColl.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"title": "1984"})

		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookId": "1", "reviewer": "Alice"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, r1}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(Equal("bookId", Variable("doc").GetField("does_not_exist"))).
			Select(Fields(FieldOf("reviewer").As("reviewer")))

		iter := client.Pipeline().Collection(outerColl.ID).
			Where(Equal("title", "1984")).
			Define(AliasedExpressions(CurrentDocument().As("doc"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews"))).
			Select(Fields("title", "reviews")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "reviews": []any{}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("test3LevelDeepJoin", func(t *testing.T) {
		publishersColl := client.Collection(collectionIDs.New())
		booksColl := client.Collection(collectionIDs.New())
		reviewsColl := client.Collection(collectionIDs.New())

		p1 := publishersColl.Doc("p1")
		h.mustCreate(p1, map[string]interface{}{"publisherId": "pub1", "name": "Penguin"})
		b1 := booksColl.Doc("b1")
		h.mustCreate(b1, map[string]interface{}{"bookId": "book1", "publisherId": "pub1", "title": "1984"})
		r1 := reviewsColl.Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"bookId": "book1", "reviewer": "Alice"})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{p1, b1, r1}) })

		reviewsSub := client.Pipeline().Collection(reviewsColl.ID).
			Where(And(
				Equal("bookId", Variable("book_id")),
				Equal(Variable("pub_name"), "Penguin"),
			)).
			Select(Fields(FieldOf("reviewer").As("reviewer")))

		booksSub := client.Pipeline().Collection(booksColl.ID).
			Where(Equal("publisherId", Variable("pub_id"))).
			Define(AliasedExpressions(FieldOf("bookId").As("book_id"))).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews"))).
			Select(Fields("title", "reviews"))

		iter := client.Pipeline().Collection(publishersColl.ID).
			Where(Equal("publisherId", "pub1")).
			Define(AliasedExpressions(FieldOf("publisherId").As("pub_id"), FieldOf("name").As("pub_name"))).
			AddFields(Selectables(booksSub.ToArrayExpression().As("books"))).
			Select(Fields("name", "books")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"name": "Penguin", "books": []any{
				map[string]any{"title": "1984", "reviews": []any{"Alice"}},
			}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testDeepAggregation", func(t *testing.T) {
		outerColl := client.Collection(collectionIDs.New())
		innerColl := client.Collection(collectionIDs.New())

		doc1 := outerColl.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"id": "1"})
		doc2 := outerColl.Doc("doc2")
		h.mustCreate(doc2, map[string]interface{}{"id": "2"})

		i1 := innerColl.Doc("i1")
		h.mustCreate(i1, map[string]interface{}{"outer_id": "1", "score": 10})
		i2 := innerColl.Doc("i2")
		h.mustCreate(i2, map[string]interface{}{"outer_id": "2", "score": 20})
		i3 := innerColl.Doc("i3")
		h.mustCreate(i3, map[string]interface{}{"outer_id": "1", "score": 30})

		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, doc2, i1, i2, i3}) })

		innerSub := client.Pipeline().Collection(innerColl.ID).
			Where(Equal("outer_id", Variable("oid"))).
			Aggregate(Accumulators(Average("score").As("s")))

		iter := client.Pipeline().Collection(outerColl.ID).
			Define(AliasedExpressions(FieldOf("id").As("oid"))).
			AddFields(Selectables(innerSub.ToScalarExpression().As("doc_score"))).
			Aggregate(Accumulators(Sum("doc_score").As("total_score"))).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"total_score": float64(40.0)},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testPipelineStageSupport10Layers", func(t *testing.T) {
		coll := client.Collection(collectionIDs.New())
		doc1 := coll.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"val": "hello"})
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1}) })

		currentSubquery := client.Pipeline().Collection(coll.ID).Limit(1).Select(Fields(FieldOf("val").As("val")))

		for i := 0; i < 9; i++ {
			currentSubquery = client.Pipeline().Collection(coll.ID).
				Limit(1).
				AddFields(Selectables(currentSubquery.ToArrayExpression().As(fmt.Sprintf("nested_%d", i)))).
				Select(Fields(fmt.Sprintf("nested_%d", i)))
		}

		iter := currentSubquery.Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(docs) == 0 {
			t.Fatalf("expected non-empty results")
		}
	})

	t.Run("testStandardSubcollectionQuery", func(t *testing.T) {
		coll := client.Collection(collectionIDs.New())
		doc1 := coll.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"title": "1984"})
		r1 := doc1.Collection("reviews").Doc("r1")
		h.mustCreate(r1, map[string]interface{}{"reviewer": "Alice"})
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, r1}) })

		reviewsSub := Subcollection("reviews").Select(Fields(FieldOf("reviewer").As("reviewer")))

		iter := client.Pipeline().Collection(coll.ID).
			Where(Equal("title", "1984")).
			AddFields(Selectables(reviewsSub.ToArrayExpression().As("reviews"))).
			Select(Fields("title", "reviews")).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"title": "1984", "reviews": []any{"Alice"}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testMissingSubcollection", func(t *testing.T) {
		coll := client.Collection(collectionIDs.New())
		doc1 := coll.Doc("doc1")
		h.mustCreate(doc1, map[string]interface{}{"id": "no_subcollection_here"})
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1}) })

		missingSub := Subcollection("does_not_exist").Select(Fields(Variable("p").As("sub_p")))

		iter := client.Pipeline().Collection(coll.ID).
			Define(AliasedExpressions(CurrentDocument().As("p"))).
			Select(Fields(missingSub.ToArrayExpression().As("missing_data"))).
			Limit(1).
			Execute(ctx).Results()
		defer iter.Stop()

		docs, err := iter.GetAll()
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		want := []map[string]any{
			{"missing_data": []any{}},
		}
		containsExactly(t, docs, want)
	})

	t.Run("testDirectExecutionOfSubcollectionPipeline", func(t *testing.T) {
		sub := Subcollection("reviews")

		iter := sub.Execute(ctx).Results()
		_, err := iter.GetAll()
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !errors.Is(err, ErrPipelineWithoutDatabase) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("testUnionWithSubqueryThrows", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				// We expect either a panic from union validation or err on execute.
				// In Go we check err on the pipeline.
			}
		}()
		p1 := client.Pipeline().Collection(coll.ID)
		p2 := Subcollection("subcollection")
		p1.Union(p2)
		if p1.err == nil {
			t.Fatalf("expected error on Union, got nil")
		}
		if !errors.Is(p1.err, ErrRelativeScopeUnionUnsupported) {
			t.Fatalf("unexpected error: %v", p1.err)
		}
	})
}

var bookDocs = map[string]map[string]any{
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
