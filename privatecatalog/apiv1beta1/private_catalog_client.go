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

package privatecatalog

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"

	privatecatalogpb "cloud.google.com/go/privatecatalog/apiv1beta1/privatecatalogpb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	httptransport "google.golang.org/api/transport/http"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	SearchCatalogs []gax.CallOption
	SearchProducts []gax.CallOption
	SearchVersions []gax.CallOption
}

func defaultGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("cloudprivatecatalog.googleapis.com:443"),
		internaloption.WithDefaultEndpointTemplate("cloudprivatecatalog.UNIVERSE_DOMAIN:443"),
		internaloption.WithDefaultMTLSEndpoint("cloudprivatecatalog.mtls.googleapis.com:443"),
		internaloption.WithDefaultUniverseDomain("googleapis.com"),
		internaloption.WithDefaultAudience("https://cloudprivatecatalog.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		internaloption.EnableJwtWithScope(),
		internaloption.EnableNewAuthLibrary(),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		SearchCatalogs: []gax.CallOption{},
		SearchProducts: []gax.CallOption{},
		SearchVersions: []gax.CallOption{},
	}
}

func defaultRESTCallOptions() *CallOptions {
	return &CallOptions{
		SearchCatalogs: []gax.CallOption{},
		SearchProducts: []gax.CallOption{},
		SearchVersions: []gax.CallOption{},
	}
}

// internalClient is an interface that defines the methods available from Cloud Private Catalog API.
type internalClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	SearchCatalogs(context.Context, *privatecatalogpb.SearchCatalogsRequest, ...gax.CallOption) *CatalogIterator
	SearchProducts(context.Context, *privatecatalogpb.SearchProductsRequest, ...gax.CallOption) *ProductIterator
	SearchVersions(context.Context, *privatecatalogpb.SearchVersionsRequest, ...gax.CallOption) *VersionIterator
}

// Client is a client for interacting with Cloud Private Catalog API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// PrivateCatalog allows catalog consumers to retrieve Catalog, Product
// and Version resources under a target resource context.
//
// Catalog is computed based on the Associations linked to the target
// resource and its ancestors. Each association’s
// google.cloud.privatecatalogproducer.v1beta.Catalog is transformed into a
// Catalog. If multiple associations have the same parent
// google.cloud.privatecatalogproducer.v1beta.Catalog, they are
// de-duplicated into one Catalog. Users must have
// cloudprivatecatalog.catalogTargets.get IAM permission on the resource
// context in order to access catalogs. Catalog contains the resource name and
// a subset of data of the original
// google.cloud.privatecatalogproducer.v1beta.Catalog.
//
// Product is child resource of the catalog. A Product contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Product.
//
// Version is child resource of the product. A Version contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Version.
type Client struct {
	// The internal transport-dependent client.
	internalClient internalClient

	// The call options for this service.
	CallOptions *CallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated: Connections are now pooled so this method does not always
// return the same resource.
func (c *Client) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// SearchCatalogs search Catalog resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *Client) SearchCatalogs(ctx context.Context, req *privatecatalogpb.SearchCatalogsRequest, opts ...gax.CallOption) *CatalogIterator {
	return c.internalClient.SearchCatalogs(ctx, req, opts...)
}

// SearchProducts search Product resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *Client) SearchProducts(ctx context.Context, req *privatecatalogpb.SearchProductsRequest, opts ...gax.CallOption) *ProductIterator {
	return c.internalClient.SearchProducts(ctx, req, opts...)
}

// SearchVersions search Version resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *Client) SearchVersions(ctx context.Context, req *privatecatalogpb.SearchVersionsRequest, opts ...gax.CallOption) *VersionIterator {
	return c.internalClient.SearchVersions(ctx, req, opts...)
}

// gRPCClient is a client for interacting with Cloud Private Catalog API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type gRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// Points back to the CallOptions field of the containing Client
	CallOptions **CallOptions

	// The gRPC API client.
	client privatecatalogpb.PrivateCatalogClient

	// The x-goog-* metadata to be sent with each request.
	xGoogHeaders []string

	logger *slog.Logger
}

// NewClient creates a new private catalog client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// PrivateCatalog allows catalog consumers to retrieve Catalog, Product
// and Version resources under a target resource context.
//
// Catalog is computed based on the Associations linked to the target
// resource and its ancestors. Each association’s
// google.cloud.privatecatalogproducer.v1beta.Catalog is transformed into a
// Catalog. If multiple associations have the same parent
// google.cloud.privatecatalogproducer.v1beta.Catalog, they are
// de-duplicated into one Catalog. Users must have
// cloudprivatecatalog.catalogTargets.get IAM permission on the resource
// context in order to access catalogs. Catalog contains the resource name and
// a subset of data of the original
// google.cloud.privatecatalogproducer.v1beta.Catalog.
//
// Product is child resource of the catalog. A Product contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Product.
//
// Version is child resource of the product. A Version contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Version.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	clientOpts := defaultGRPCClientOptions()
	if newClientHook != nil {
		hookOpts, err := newClientHook(ctx, clientHookParams{})
		if err != nil {
			return nil, err
		}
		clientOpts = append(clientOpts, hookOpts...)
	}

	connPool, err := gtransport.DialPool(ctx, append(clientOpts, opts...)...)
	if err != nil {
		return nil, err
	}
	client := Client{CallOptions: defaultCallOptions()}

	c := &gRPCClient{
		connPool:    connPool,
		client:      privatecatalogpb.NewPrivateCatalogClient(connPool),
		CallOptions: &client.CallOptions,
		logger:      internaloption.GetLogger(opts),
	}
	c.setGoogleClientInfo()

	client.internalClient = c

	return &client, nil
}

// Connection returns a connection to the API service.
//
// Deprecated: Connections are now pooled so this method does not always
// return the same resource.
func (c *gRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *gRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", gax.GoVersion}, keyval...)
	kv = append(kv, "gapic", getVersionClient(), "gax", gax.Version, "grpc", grpc.Version, "pb", protoVersion)
	c.xGoogHeaders = []string{
		"x-goog-api-client", gax.XGoogHeader(kv...),
	}
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *gRPCClient) Close() error {
	return c.connPool.Close()
}

// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type restClient struct {
	// The http endpoint to connect to.
	endpoint string

	// The http client.
	httpClient *http.Client

	// The x-goog-* headers to be sent with each request.
	xGoogHeaders []string

	// Points back to the CallOptions field of the containing Client
	CallOptions **CallOptions

	logger *slog.Logger
}

// NewRESTClient creates a new private catalog rest client.
//
// PrivateCatalog allows catalog consumers to retrieve Catalog, Product
// and Version resources under a target resource context.
//
// Catalog is computed based on the Associations linked to the target
// resource and its ancestors. Each association’s
// google.cloud.privatecatalogproducer.v1beta.Catalog is transformed into a
// Catalog. If multiple associations have the same parent
// google.cloud.privatecatalogproducer.v1beta.Catalog, they are
// de-duplicated into one Catalog. Users must have
// cloudprivatecatalog.catalogTargets.get IAM permission on the resource
// context in order to access catalogs. Catalog contains the resource name and
// a subset of data of the original
// google.cloud.privatecatalogproducer.v1beta.Catalog.
//
// Product is child resource of the catalog. A Product contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Product.
//
// Version is child resource of the product. A Version contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Version.
func NewRESTClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	clientOpts := append(defaultRESTClientOptions(), opts...)
	httpClient, endpoint, err := httptransport.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}

	callOpts := defaultRESTCallOptions()
	c := &restClient{
		endpoint:    endpoint,
		httpClient:  httpClient,
		CallOptions: &callOpts,
		logger:      internaloption.GetLogger(opts),
	}
	c.setGoogleClientInfo()

	return &Client{internalClient: c, CallOptions: callOpts}, nil
}

func defaultRESTClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("https://cloudprivatecatalog.googleapis.com"),
		internaloption.WithDefaultEndpointTemplate("https://cloudprivatecatalog.UNIVERSE_DOMAIN"),
		internaloption.WithDefaultMTLSEndpoint("https://cloudprivatecatalog.mtls.googleapis.com"),
		internaloption.WithDefaultUniverseDomain("googleapis.com"),
		internaloption.WithDefaultAudience("https://cloudprivatecatalog.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		internaloption.EnableNewAuthLibrary(),
	}
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *restClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", gax.GoVersion}, keyval...)
	kv = append(kv, "gapic", getVersionClient(), "gax", gax.Version, "rest", "UNKNOWN", "pb", protoVersion)
	c.xGoogHeaders = []string{
		"x-goog-api-client", gax.XGoogHeader(kv...),
	}
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *restClient) Close() error {
	// Replace httpClient with nil to force cleanup.
	c.httpClient = nil
	return nil
}

// Connection returns a connection to the API service.
//
// Deprecated: This method always returns nil.
func (c *restClient) Connection() *grpc.ClientConn {
	return nil
}
func (c *gRPCClient) SearchCatalogs(ctx context.Context, req *privatecatalogpb.SearchCatalogsRequest, opts ...gax.CallOption) *CatalogIterator {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).SearchCatalogs[0:len((*c.CallOptions).SearchCatalogs):len((*c.CallOptions).SearchCatalogs)], opts...)
	it := &CatalogIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchCatalogsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Catalog, string, error) {
		resp := &privatecatalogpb.SearchCatalogsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = executeRPC(ctx, c.client.SearchCatalogs, req, settings.GRPC, c.logger, "SearchCatalogs")
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetCatalogs(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

func (c *gRPCClient) SearchProducts(ctx context.Context, req *privatecatalogpb.SearchProductsRequest, opts ...gax.CallOption) *ProductIterator {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).SearchProducts[0:len((*c.CallOptions).SearchProducts):len((*c.CallOptions).SearchProducts)], opts...)
	it := &ProductIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchProductsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Product, string, error) {
		resp := &privatecatalogpb.SearchProductsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = executeRPC(ctx, c.client.SearchProducts, req, settings.GRPC, c.logger, "SearchProducts")
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetProducts(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

func (c *gRPCClient) SearchVersions(ctx context.Context, req *privatecatalogpb.SearchVersionsRequest, opts ...gax.CallOption) *VersionIterator {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).SearchVersions[0:len((*c.CallOptions).SearchVersions):len((*c.CallOptions).SearchVersions)], opts...)
	it := &VersionIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchVersionsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Version, string, error) {
		resp := &privatecatalogpb.SearchVersionsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = executeRPC(ctx, c.client.SearchVersions, req, settings.GRPC, c.logger, "SearchVersions")
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetVersions(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

// SearchCatalogs search Catalog resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *restClient) SearchCatalogs(ctx context.Context, req *privatecatalogpb.SearchCatalogsRequest, opts ...gax.CallOption) *CatalogIterator {
	it := &CatalogIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchCatalogsRequest)
	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Catalog, string, error) {
		resp := &privatecatalogpb.SearchCatalogsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		baseUrl, err := url.Parse(c.endpoint)
		if err != nil {
			return nil, "", err
		}
		baseUrl.Path += fmt.Sprintf("/v1beta1/%v/catalogs:search", req.GetResource())

		params := url.Values{}
		params.Add("$alt", "json;enum-encoding=int")
		if req.GetPageSize() != 0 {
			params.Add("pageSize", fmt.Sprintf("%v", req.GetPageSize()))
		}
		if req.GetPageToken() != "" {
			params.Add("pageToken", fmt.Sprintf("%v", req.GetPageToken()))
		}
		if req.GetQuery() != "" {
			params.Add("query", fmt.Sprintf("%v", req.GetQuery()))
		}

		baseUrl.RawQuery = params.Encode()

		// Build HTTP headers from client and context metadata.
		hds := append(c.xGoogHeaders, "Content-Type", "application/json")
		headers := gax.BuildHeaders(ctx, hds...)
		e := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			if settings.Path != "" {
				baseUrl.Path = settings.Path
			}
			httpReq, err := http.NewRequest("GET", baseUrl.String(), nil)
			if err != nil {
				return err
			}
			httpReq.Header = headers

			buf, err := executeHTTPRequest(ctx, c.httpClient, httpReq, c.logger, nil, "SearchCatalogs")
			if err != nil {
				return err
			}
			if err := unm.Unmarshal(buf, resp); err != nil {
				return err
			}

			return nil
		}, opts...)
		if e != nil {
			return nil, "", e
		}
		it.Response = resp
		return resp.GetCatalogs(), resp.GetNextPageToken(), nil
	}

	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

// SearchProducts search Product resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *restClient) SearchProducts(ctx context.Context, req *privatecatalogpb.SearchProductsRequest, opts ...gax.CallOption) *ProductIterator {
	it := &ProductIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchProductsRequest)
	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Product, string, error) {
		resp := &privatecatalogpb.SearchProductsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		baseUrl, err := url.Parse(c.endpoint)
		if err != nil {
			return nil, "", err
		}
		baseUrl.Path += fmt.Sprintf("/v1beta1/%v/products:search", req.GetResource())

		params := url.Values{}
		params.Add("$alt", "json;enum-encoding=int")
		if req.GetPageSize() != 0 {
			params.Add("pageSize", fmt.Sprintf("%v", req.GetPageSize()))
		}
		if req.GetPageToken() != "" {
			params.Add("pageToken", fmt.Sprintf("%v", req.GetPageToken()))
		}
		if req.GetQuery() != "" {
			params.Add("query", fmt.Sprintf("%v", req.GetQuery()))
		}

		baseUrl.RawQuery = params.Encode()

		// Build HTTP headers from client and context metadata.
		hds := append(c.xGoogHeaders, "Content-Type", "application/json")
		headers := gax.BuildHeaders(ctx, hds...)
		e := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			if settings.Path != "" {
				baseUrl.Path = settings.Path
			}
			httpReq, err := http.NewRequest("GET", baseUrl.String(), nil)
			if err != nil {
				return err
			}
			httpReq.Header = headers

			buf, err := executeHTTPRequest(ctx, c.httpClient, httpReq, c.logger, nil, "SearchProducts")
			if err != nil {
				return err
			}
			if err := unm.Unmarshal(buf, resp); err != nil {
				return err
			}

			return nil
		}, opts...)
		if e != nil {
			return nil, "", e
		}
		it.Response = resp
		return resp.GetProducts(), resp.GetNextPageToken(), nil
	}

	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

// SearchVersions search Version resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *restClient) SearchVersions(ctx context.Context, req *privatecatalogpb.SearchVersionsRequest, opts ...gax.CallOption) *VersionIterator {
	it := &VersionIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchVersionsRequest)
	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Version, string, error) {
		resp := &privatecatalogpb.SearchVersionsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		baseUrl, err := url.Parse(c.endpoint)
		if err != nil {
			return nil, "", err
		}
		baseUrl.Path += fmt.Sprintf("/v1beta1/%v/versions:search", req.GetResource())

		params := url.Values{}
		params.Add("$alt", "json;enum-encoding=int")
		if req.GetPageSize() != 0 {
			params.Add("pageSize", fmt.Sprintf("%v", req.GetPageSize()))
		}
		if req.GetPageToken() != "" {
			params.Add("pageToken", fmt.Sprintf("%v", req.GetPageToken()))
		}
		params.Add("query", fmt.Sprintf("%v", req.GetQuery()))

		baseUrl.RawQuery = params.Encode()

		// Build HTTP headers from client and context metadata.
		hds := append(c.xGoogHeaders, "Content-Type", "application/json")
		headers := gax.BuildHeaders(ctx, hds...)
		e := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			if settings.Path != "" {
				baseUrl.Path = settings.Path
			}
			httpReq, err := http.NewRequest("GET", baseUrl.String(), nil)
			if err != nil {
				return err
			}
			httpReq.Header = headers

			buf, err := executeHTTPRequest(ctx, c.httpClient, httpReq, c.logger, nil, "SearchVersions")
			if err != nil {
				return err
			}
			if err := unm.Unmarshal(buf, resp); err != nil {
				return err
			}

			return nil
		}, opts...)
		if e != nil {
			return nil, "", e
		}
		it.Response = resp
		return resp.GetVersions(), resp.GetNextPageToken(), nil
	}

	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}
