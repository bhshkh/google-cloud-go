// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go_gapic. DO NOT EDIT.

package conversions_test

import (
	"context"

	conversions "cloud.google.com/go/shopping/merchant/conversions/apiv1beta"
	conversionspb "cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb"
	"google.golang.org/api/iterator"
)

func ExampleNewConversionSourcesClient() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExampleNewConversionSourcesRESTClient() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExampleConversionSourcesClient_CreateConversionSource() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &conversionspb.CreateConversionSourceRequest{
		// TODO: Fill request struct fields.
		// See https://pkg.go.dev/cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb#CreateConversionSourceRequest.
	}
	resp, err := c.CreateConversionSource(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConversionSourcesClient_DeleteConversionSource() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &conversionspb.DeleteConversionSourceRequest{
		// TODO: Fill request struct fields.
		// See https://pkg.go.dev/cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb#DeleteConversionSourceRequest.
	}
	err = c.DeleteConversionSource(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleConversionSourcesClient_GetConversionSource() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &conversionspb.GetConversionSourceRequest{
		// TODO: Fill request struct fields.
		// See https://pkg.go.dev/cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb#GetConversionSourceRequest.
	}
	resp, err := c.GetConversionSource(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConversionSourcesClient_ListConversionSources() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &conversionspb.ListConversionSourcesRequest{
		// TODO: Fill request struct fields.
		// See https://pkg.go.dev/cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb#ListConversionSourcesRequest.
	}
	it := c.ListConversionSources(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp

		// If you need to access the underlying RPC response,
		// you can do so by casting the `Response` as below.
		// Otherwise, remove this line. Only populated after
		// first call to Next(). Not safe for concurrent access.
		_ = it.Response.(*conversionspb.ListConversionSourcesResponse)
	}
}

func ExampleConversionSourcesClient_UndeleteConversionSource() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &conversionspb.UndeleteConversionSourceRequest{
		// TODO: Fill request struct fields.
		// See https://pkg.go.dev/cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb#UndeleteConversionSourceRequest.
	}
	resp, err := c.UndeleteConversionSource(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConversionSourcesClient_UpdateConversionSource() {
	ctx := context.Background()
	// This snippet has been automatically generated and should be regarded as a code template only.
	// It will require modifications to work:
	// - It may require correct/in-range values for request initialization.
	// - It may require specifying regional endpoints when creating the service client as shown in:
	//   https://pkg.go.dev/cloud.google.com/go#hdr-Client_Options
	c, err := conversions.NewConversionSourcesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &conversionspb.UpdateConversionSourceRequest{
		// TODO: Fill request struct fields.
		// See https://pkg.go.dev/cloud.google.com/go/shopping/merchant/conversions/apiv1beta/conversionspb#UpdateConversionSourceRequest.
	}
	resp, err := c.UpdateConversionSource(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
