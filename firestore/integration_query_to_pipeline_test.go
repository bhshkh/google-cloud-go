// Copyright 2024 Google LLC
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
	"math"
	"testing"
)

func testCollectionWithDocsQTP(t *testing.T, docs map[string]map[string]any) (*CollectionRef, []*DocumentRef) {
	client := integrationClient(t)
	coll := client.Collection(collectionIDs.New())
	h := testHelper{t}
	var docRefs []*DocumentRef
	for id, data := range docs {
		doc := coll.Doc(id)
		docRefs = append(docRefs, doc)
		h.mustCreate(doc, data)
	}
	return coll, docRefs
}

func verifyResultsQTP(t *testing.T, actual []*PipelineResult, expected ...map[string]any) {
	t.Helper()
	if expected == nil {
		expected = []map[string]any{}
	}
	var expectedAny []map[string]any
	for _, e := range expected {
		expectedAny = append(expectedAny, e)
	}
	isEqualTo(t, actual, expectedAny)
}

func TestIntegration_QueryToPipeline(t *testing.T) {
	skipIfEdition(t, "Query to Pipeline", editionStandard)
	ctx := context.Background()
	client := integrationClient(t)

	t.Run("supportsDefaultQuery", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		iter := client.Pipeline().CreateFromQuery(collRef).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsFilteredQuery", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("foo", "==", int64(1))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsFilteredQueryWithFieldPath", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.WherePath([]string{"foo"}, "==", int64(1))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsOrderedQueryWithDefaultOrder", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)}, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsOrderedQueryWithAsc", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)}, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsOrderedQueryWithDesc", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Desc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)}, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsLimitQuery", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).Limit(1)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsLimitToLastQuery", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
			"3": {"foo": int64(3)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).LimitToLast(2)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)}, map[string]any{"foo": int64(3)})
	})

	t.Run("supportsStartAt", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).StartAt(int64(2))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsStartAtWithLimitToLast", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
			"3": {"foo": int64(3)},
			"4": {"foo": int64(4)},
			"5": {"foo": int64(5)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).StartAt(int64(3)).LimitToLast(4)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(3)}, map[string]any{"foo": int64(4)}, map[string]any{"foo": int64(5)})
	})

	t.Run("supportsEndAtWithLimitToLast", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
			"3": {"foo": int64(3)},
			"4": {"foo": int64(4)},
			"5": {"foo": int64(5)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).EndAt(int64(3)).LimitToLast(2)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)}, map[string]any{"foo": int64(3)})
	})

	t.Run("supportsStartAfterWithDocumentSnapshot", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1":  {"id": int64(1), "foo": int64(1), "bar": int64(1), "baz": int64(1)},
			"2":  {"id": int64(2), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"3":  {"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"4":  {"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			"5":  {"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"6":  {"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"7":  {"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			"8":  {"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"9":  {"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"10": {"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
			"11": {"id": int64(11), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
			"12": {"id": int64(12), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		docRef, err := collRef.Doc("2").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		query1 := collRef.OrderBy("foo", Asc).OrderBy("bar", Asc).OrderBy("baz", Asc).StartAfter(docRef)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot,
			map[string]any{"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			map[string]any{"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			map[string]any{"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
			map[string]any{"id": int64(11), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(12), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
		)
	})

	t.Run("supportsStartAtWithDocumentSnapshot", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1":  {"id": int64(1), "foo": int64(1), "bar": int64(1), "baz": int64(1)},
			"2":  {"id": int64(2), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"3":  {"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"4":  {"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			"5":  {"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"6":  {"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"7":  {"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			"8":  {"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"9":  {"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"10": {"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
			"11": {"id": int64(11), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
			"12": {"id": int64(12), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		docRef, err := collRef.Doc("2").Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		query1 := collRef.OrderBy("foo", Asc).OrderBy("bar", Asc).OrderBy("baz", Asc).StartAt(docRef)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot,
			map[string]any{"id": int64(2), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			map[string]any{"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			map[string]any{"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
			map[string]any{"id": int64(11), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(12), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
		)
	})

	t.Run("supportsStartAfter", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).StartAfter(int64(1))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsEndAt", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).EndAt(int64(1))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsEndBefore", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).EndBefore(int64(2))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsPagination", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).Limit(1)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})

		fooVal := snapshot[0].Data()["foo"]
		query2 := query1.StartAfter(fooVal)
		iter2 := client.Pipeline().CreateFromQuery(query2).Execute(ctx).Results()
		defer iter2.Stop()
		snapshot2, err := iter2.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot2, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsPaginationOnDocumentIds", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
			"2": {"foo": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("foo", Asc).OrderBy(DocumentID, Asc).Limit(1)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})

		fooVal := snapshot[0].Data()["foo"]
		query2 := query1.StartAfter(fooVal, snapshot[0].Ref().ID)
		iter2 := client.Pipeline().CreateFromQuery(query2).Execute(ctx).Results()
		defer iter2.Stop()
		snapshot2, err := iter2.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot2, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsCollectionGroups", func(t *testing.T) {
		collRef, _ := testCollectionWithDocsQTP(t, map[string]map[string]any{})
		collectionGroupId := collRef.ID + "group"

		doc1 := client.Doc(collRef.ID + "/foo/" + collectionGroupId + "/doc1")
		doc2 := client.Doc(collRef.ID + "/bar/baz/boo/" + collectionGroupId + "/doc2")
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, doc2}) })
		testHelper{t}.mustSet(doc1, map[string]interface{}{"foo": int64(1)})
		testHelper{t}.mustSet(doc2, map[string]interface{}{"bar": int64(1)})

		query1 := client.CollectionGroup(collectionGroupId).OrderBy(DocumentID, Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}

		verifyResultsQTP(t, snapshot, map[string]any{"bar": int64(1)}, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsQueryOverCollectionPathWithSpecialCharacters", func(t *testing.T) {
		collRef, _ := testCollectionWithDocsQTP(t, map[string]map[string]any{})
		collectionWithSpecials := collRef.Doc("so!@#$%^&*()_+special").Collection("so!@#$%^&*()_+special")
		doc1 := collectionWithSpecials.NewDoc()
		doc2 := collectionWithSpecials.NewDoc()
		t.Cleanup(func() { deleteDocuments([]*DocumentRef{doc1, doc2}) })
		testHelper{t}.mustCreate(doc1, map[string]interface{}{"foo": int64(1)})
		testHelper{t}.mustCreate(doc2, map[string]interface{}{"foo": int64(2)})

		query := collectionWithSpecials.OrderBy("foo", Asc)
		iter := client.Pipeline().CreateFromQuery(query).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}

		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)}, map[string]any{"foo": int64(2)})
	})

	t.Run("supportsMultipleInequalityOnSameField", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"01": {"id": int64(1), "foo": int64(1), "bar": int64(1), "baz": int64(1)},
			"02": {"id": int64(2), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"03": {"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"04": {"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			"05": {"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"06": {"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"07": {"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			"08": {"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"09": {"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"10": {"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
			"11": {"id": int64(11), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
			"12": {"id": int64(12), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })
		query1 := collRef.WhereEntity(AndFilter{[]EntityFilter{
			PropertyFilter{Path: "id", Operator: ">", Value: int64(2)},
			PropertyFilter{Path: "id", Operator: "<=", Value: int64(10)},
		}})
		iter := client.Pipeline().CreateFromQuery(query1.OrderBy("id", Asc)).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot,
			map[string]any{"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			map[string]any{"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			map[string]any{"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			map[string]any{"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			map[string]any{"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
		)
	})

	t.Run("supportsMultipleInequalityOnDifferentFields", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"01": {"id": int64(1), "foo": int64(1), "bar": int64(1), "baz": int64(1)},
			"02": {"id": int64(2), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"03": {"id": int64(3), "foo": int64(1), "bar": int64(1), "baz": int64(2)},
			"04": {"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			"05": {"id": int64(5), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"06": {"id": int64(6), "foo": int64(1), "bar": int64(2), "baz": int64(2)},
			"07": {"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			"08": {"id": int64(8), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"09": {"id": int64(9), "foo": int64(2), "bar": int64(1), "baz": int64(2)},
			"10": {"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
			"11": {"id": int64(11), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
			"12": {"id": int64(12), "foo": int64(2), "bar": int64(2), "baz": int64(2)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })
		query1 := collRef.WhereEntity(AndFilter{[]EntityFilter{
			PropertyFilter{Path: "id", Operator: ">=", Value: int64(2)},
			PropertyFilter{Path: "baz", Operator: "<", Value: int64(2)},
		}})
		iter := client.Pipeline().CreateFromQuery(query1.OrderBy("id", Asc)).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot,
			map[string]any{"id": int64(4), "foo": int64(1), "bar": int64(2), "baz": int64(1)},
			map[string]any{"id": int64(7), "foo": int64(2), "bar": int64(1), "baz": int64(1)},
			map[string]any{"id": int64(10), "foo": int64(2), "bar": int64(2), "baz": int64(1)},
		)
	})

	t.Run("supportsCollectionGroupQuery", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		iter := client.Pipeline().CreateFromQuery(client.CollectionGroup(collRef.ID)).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1)})
	})

	t.Run("supportsEqNan", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": math.NaN()},
			"2": {"foo": int64(2), "bar": int64(1)},
			"3": {"foo": int64(3), "bar": "bar"},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "==", math.NaN())
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": math.NaN()})
	})

	t.Run("supportsNeqNan", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": math.NaN()},
			"2": {"foo": int64(2), "bar": int64(1)},
			"3": {"foo": int64(3), "bar": "bar"},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "!=", math.NaN()).OrderBy("foo", Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2), "bar": int64(1)}, map[string]any{"foo": int64(3), "bar": "bar"})
	})

	t.Run("supportsEqNull", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": nil},
			"2": {"foo": int64(2), "bar": int64(1)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "==", nil)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": nil})
	})

	t.Run("supportsNeqNull", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": nil},
			"2": {"foo": int64(2), "bar": int64(1)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "!=", nil)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2), "bar": int64(1)})
	})

	t.Run("supportsNeq", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(0)},
			"2": {"foo": int64(2), "bar": int64(1)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "!=", int64(0))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2), "bar": int64(1)})
	})

	t.Run("supportsArrayContains", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": []interface{}{int64(0), int64(2), int64(4), int64(6)}},
			"2": {"foo": int64(2), "bar": []interface{}{int64(1), int64(3), int64(5), int64(7)}},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "array-contains", int64(4))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": []interface{}{int64(0), int64(2), int64(4), int64(6)}})
	})

	t.Run("supportsArrayContainsAny", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": []interface{}{int64(0), int64(2), int64(4), int64(6)}},
			"2": {"foo": int64(2), "bar": []interface{}{int64(1), int64(3), int64(5), int64(7)}},
			"3": {"foo": int64(3), "bar": []interface{}{int64(10), int64(20), int64(30), int64(40)}},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "array-contains-any", []int64{4, 5})
		iter := client.Pipeline().CreateFromQuery(query1.OrderBy("foo", Asc)).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot,
			map[string]any{"foo": int64(1), "bar": []interface{}{int64(0), int64(2), int64(4), int64(6)}},
			map[string]any{"foo": int64(2), "bar": []interface{}{int64(1), int64(3), int64(5), int64(7)}},
		)
	})

	t.Run("supportsIn", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(2)},
			"2": {"foo": int64(2)},
			"3": {"foo": int64(3), "bar": int64(10)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "in", []int64{0, 10, 20})
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(3), "bar": int64(10)})
	})

	t.Run("supportsInWith1", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(2)},
			"2": {"foo": int64(2)},
			"3": {"foo": int64(3), "bar": int64(10)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "in", []int64{2})
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": int64(2)})
	})

	t.Run("supportsNotIn", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(2)},
			"2": {"foo": int64(2), "bar": int64(1)},
			"3": {"foo": int64(3), "bar": int64(10)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "not-in", []int64{0, 10, 20}).OrderBy("foo", Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": int64(2)}, map[string]any{"foo": int64(2), "bar": int64(1)})
	})

	t.Run("supportsNotInWith1", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(2)},
			"2": {"foo": int64(2)},
			"3": {"foo": int64(3), "bar": int64(10)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "not-in", []int64{2}).OrderBy("foo", Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		if getCurrentEdition() == editionEnterprise {
			verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)}, map[string]any{"foo": int64(3), "bar": int64(10)})
		} else {
			verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(3), "bar": int64(10)})
		}
	})

	t.Run("supportsOrOperator", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(2)},
			"2": {"foo": int64(2), "bar": int64(0)},
			"3": {"foo": int64(3), "bar": int64(10)},
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.WhereEntity(OrFilter{[]EntityFilter{
			PropertyFilter{Path: "bar", Operator: "==", Value: int64(2)},
			PropertyFilter{Path: "foo", Operator: "==", Value: int64(3)},
		}}).OrderBy("foo", Asc)

		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": int64(2)}, map[string]any{"foo": int64(3), "bar": int64(10)})
	})

	t.Run("testNotEqualIncludesMissingField", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(1)},
			"2": {"foo": int64(2)}, // Missing "bar"
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "!=", int64(1))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)})
	})

	t.Run("testNotInIncludesMissingField", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(1)},
			"2": {"foo": int64(2)}, // Missing "bar"
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "not-in", []int64{1})
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(2)})
	})

	t.Run("testInequalityMaintainsExistenceFilter", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(0)},
			"2": {"foo": int64(2)}, // Missing "bar"
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.Where("bar", "<", int64(1))
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": int64(0)})
	})

	t.Run("testExplicitOrderMaintainsExistenceFilter", func(t *testing.T) {
		collRef, docRefs := testCollectionWithDocsQTP(t, map[string]map[string]any{
			"1": {"foo": int64(1), "bar": int64(1)},
			"2": {"foo": int64(2)}, // Missing "bar"
		})
		t.Cleanup(func() { deleteDocuments(docRefs) })

		query1 := collRef.OrderBy("bar", Asc)
		iter := client.Pipeline().CreateFromQuery(query1).Execute(ctx).Results()
		defer iter.Stop()
		snapshot, err := iter.GetAll()
		if err != nil {
			t.Fatal(err)
		}
		verifyResultsQTP(t, snapshot, map[string]any{"foo": int64(1), "bar": int64(1)})
	})
}
