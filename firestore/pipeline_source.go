// Copyright 2025 Google LLC
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
	"fmt"
	"reflect"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// PipelineSource is a factory for creating Pipeline instances.
// It is obtained by calling [Client.Pipeline()].
type PipelineSource struct {
	client *Client
}

// CollectionHints provides hints to the query planner.
type CollectionHints map[string]any

// WithForceIndex specifies an index to force the query to use.
func (ch CollectionHints) WithForceIndex(index string) CollectionHints {
	ch["force_index"] = index
	return ch
}

// WithIgnoreIndexFields specifies fields to ignore when selecting an index.
func (ch CollectionHints) WithIgnoreIndexFields(fields ...string) CollectionHints {
	ch["ignore_index_fields"] = fields
	return ch
}

// CollectionOptions provides options for the Collection stage.
type CollectionOptions struct {
	Hints CollectionHints
}

// Collection creates a new [Pipeline] that operates on the specified Firestore collection.
func (ps *PipelineSource) Collection(path string, opts *CollectionOptions) *Pipeline {
	return newPipeline(ps.client, newInputStageCollection(path, opts))
}

// CollectionGroupOptions provides options for the CollectionGroup stage.
type CollectionGroupOptions struct {
	Hints CollectionHints
}

// CollectionGroup creates a new [Pipeline] that operates on all documents in a group
// of collections that include the given ID, regardless of parent document.
//
// For example, consider:
// Countries/France/Cities/Paris = {population: 100}
// Countries/Canada/Cities/Montreal = {population: 90}
//
// CollectionGroup can be used to query across all "Cities" regardless of
// its parent "Countries".
func (ps *PipelineSource) CollectionGroup(collectionID string, opts *CollectionGroupOptions) *Pipeline {
	return newPipeline(ps.client, newInputStageCollectionGroup("", collectionID, opts))
}

// Database creates a new [Pipeline] that operates on all documents in the Firestore database.
func (ps *PipelineSource) Database() *Pipeline {
	return newPipeline(ps.client, newInputStageDatabase())
}

func (ps *PipelineSource) Documents(refs ...*DocumentRef) *Pipeline {
	return newPipeline(ps.client, newInputStageDocuments(refs...))
}

func optionsToProto(options any) (map[string]*pb.Value, error) {
	if options == nil {
		return nil, nil
	}

	v := reflect.ValueOf(options)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("firestore: options must be a struct or pointer to struct, got %T", options)
	}

	optsMap := make(map[string]*pb.Value)

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := typ.Field(i)
		fieldValue := v.Field(i)

		if fieldValue.IsZero() {
			continue
		}

		optionName := field.Name
		if tag, ok := field.Tag.Lookup("firestore"); ok {
			optionName = tag
		}

		pbVal, _, err := toProtoValue(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("firestore: error converting option %q: %w", optionName, err)
		}
		optsMap[optionName] = pbVal
	}

	return optsMap, nil
}
