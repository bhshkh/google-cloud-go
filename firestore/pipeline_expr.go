// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pipeline_expressions.go

type baseAliasable interface {
	As(alias string) Selectable // For ExprWithAlias
	isBaseAliasable()
}

// Expr is an interface that represents an expression in a Firestore pipeline.
// It can be a field, a constant, or the result of a function.
type Expr interface {
	// toProto() (*pb.Value, error)
	toProto() (*pb.Value, error)

	Add(other Expr) *AddExpr
	AddValue(other any) *AddExpr
	Subtract(other Expr) *SubtractExpr
	SubtractValue(other any) *SubtractExpr
	Multiply(other Expr) *MultiplyExpr
	MultiplyValue(other any) *MultiplyExpr
	Divide(other Expr) *DivideExpr
	DivideValue(other any) *DivideExpr
	Mod(other Expr) *ModExpr
	ModValue(other any) *ModExpr
	LogicalMax(other Expr) *LogicalMaxExpr
	LogicalMaxValue(other any) *LogicalMaxExpr
	LogicalMin(other Expr) *LogicalMinExpr
	LogicalMinValue(other any) *LogicalMinExpr

	// Comparison operations
	Eq(other Expr) *EqCondition
	EqValue(other any) *EqCondition
	Neq(other Expr) *NeqCondition
	NeqValue(other any) *NeqCondition
	Gt(other Expr) *GtCondition
	GtValue(other any) *GtCondition
	Gte(other Expr) *GteCondition
	GteValue(other any) *GteCondition
	Lt(other Expr) *LtCondition
	LtValue(other any) *LtCondition
	Lte(other Expr) *LteCondition
	LteValue(other any) *LteCondition
	InAny(values ...any) *InCondition
	NotInAny(values ...any) *NotCondition // Not(InCondition)
	Exists() *ExistsCondition
	IsNaN() *IsNaNCondition

	// Array functions
	ArrayConcat(elements ...any) *ArrayConcatExpr
	ArrayContains(element Expr) *ArrayContainsCondition
	ArrayContainsValue(element any) *ArrayContainsCondition
	ArrayContainsAll(elements ...Expr) *ArrayContainsAllCondition
	ArrayContainsAllValues(elements ...any) *ArrayContainsAllCondition
	ArrayContainsAny(elements ...Expr) *ArrayContainsAnyCondition
	ArrayContainsAnyValues(elements ...any) *ArrayContainsAnyCondition

	ArrayLength() *ArrayLengthExpr
	ArrayReverse() *ArrayReverseExpr
	// ArrayFilter(condition FilterCondition) *ArrayFilterExpr
	// ArrayTransform(transform *Function) *ArrayTransformExpr
	// ArrayElement() *ArrayElementExpr // Represents the current element in an array transformation

	// String functions
	CharLength() *CharLengthExpr
	ByteLength() *ByteLengthExpr
	Like(pattern Expr) *LikeCondition
	LikeString(pattern string) *LikeCondition
	RegexContains(regex Expr) *RegexContainsCondition
	RegexContainsString(regex string) *RegexContainsCondition
	RegexMatches(regex Expr) *RegexMatchCondition
	RegexMatchesString(regex string) *RegexMatchCondition
	StrContains(substring Expr) *StrContainsCondition
	StrContainsString(substring string) *StrContainsCondition
	StartsWith(prefix Expr) *StartsWithCondition
	StartsWithString(prefix string) *StartsWithCondition
	EndsWith(postfix Expr) *EndsWithCondition
	EndsWithString(postfix string) *EndsWithCondition
	StrConcat(elements ...any) *StrConcatExpr
	ToLower() *ToLowerExpr
	ToUpper() *ToUpperExpr
	Trim() *TrimExpr
	Reverse() *ReverseExpr
	ReplaceFirst(find, replace Expr) *ReplaceFirstExpr
	ReplaceFirstStrings(find, replace string) *ReplaceFirstExpr
	ReplaceAll(find, replace Expr) *ReplaceAllExpr
	ReplaceAllStrings(find, replace string) *ReplaceAllExpr

	// Map functions
	MapGet(key string) *MapGetExpr

	// Vector functions
	CosineDistance(other Expr) *CosineDistanceExpr
	CosineDistanceVector(other []float64) *CosineDistanceExpr
	EuclideanDistance(other Expr) *EuclideanDistanceExpr
	EuclideanDistanceVector(other []float64) *EuclideanDistanceExpr
	DotProduct(other Expr) *DotProductExpr
	DotProductVector(other []float64) *DotProductExpr
	VectorLength() *VectorLengthExpr

	// Timestamp functions
	TimestampToUnixMicros() *TimestampToUnixMicrosExpr
	UnixMicrosToTimestamp() *UnixMicrosToTimestampExpr
	TimestampToUnixMillis() *TimestampToUnixMillisExpr
	UnixMillisToTimestamp() *UnixMillisToTimestampExpr
	TimestampToUnixSeconds() *TimestampToUnixSecondsExpr
	UnixSecondsToTimestamp() *UnixSecondsToTimestampExpr
	TimestampAdd(unit Expr, amount Expr) *TimestampAddExpr
	TimestampAddUnitAmount(unit string, amount float64) *TimestampAddExpr
	TimestampSub(unit Expr, amount Expr) *TimestampSubExpr
	TimestampSubUnitAmount(unit string, amount float64) *TimestampSubExpr

	// Document functions
	// CollectionID() *CollectionIDExpr
	// Parent() *ParentExpr

	// Ordering
	Ascending() *Ordering
	Descending() *Ordering

	// Accumulators (these are methods on Expr, but return Accumulator types)
	Sum() *SumAccumulator
	Avg() *AvgAccumulator
	Count() *CountAccumulator
	Min() *MinAccumulator
	Max() *MaxAccumulator
}

// baseExpr provides common methods for all Expr implementations, allowing for method chaining.
type baseExpr struct {
	pbVal *pb.Value
}

// Ensure that baseExpr implements the Expr interface.
var _ Expr = (*baseExpr)(nil)

func (b *baseExpr) toProto() (*pb.Value, error)       { return b.pbVal, nil }
func (b *baseExpr) Add(other Expr) *AddExpr           { return newAddExpr(b, other) }
func (b *baseExpr) AddValue(other any) *AddExpr       { return newAddExpr(b, ConstantOf(other)) }
func (b *baseExpr) Subtract(other Expr) *SubtractExpr { return newSubtractExpr(b, other) }
func (b *baseExpr) SubtractValue(other any) *SubtractExpr {
	return newSubtractExpr(b, ConstantOf(other))
}
func (b *baseExpr) Multiply(other Expr) *MultiplyExpr { return newMultiplyExpr(b, other) }
func (b *baseExpr) MultiplyValue(other any) *MultiplyExpr {
	return newMultiplyExpr(b, ConstantOf(other))
}
func (b *baseExpr) Divide(other Expr) *DivideExpr { return newDivideExpr(b, other) }
func (b *baseExpr) DivideValue(other any) *DivideExpr {
	return newDivideExpr(b, ConstantOf(other))
}
func (b *baseExpr) Mod(other Expr) *ModExpr               { return newModExpr(b, other) }
func (b *baseExpr) ModValue(other any) *ModExpr           { return newModExpr(b, ConstantOf(other)) }
func (b *baseExpr) LogicalMax(other Expr) *LogicalMaxExpr { return newLogicalMaxExpr(b, other) }
func (b *baseExpr) LogicalMaxValue(other any) *LogicalMaxExpr {
	return newLogicalMaxExpr(b, ConstantOf(other))
}
func (b *baseExpr) LogicalMin(other Expr) *LogicalMinExpr { return newLogicalMinExpr(b, other) }
func (b *baseExpr) LogicalMinValue(other any) *LogicalMinExpr {
	return newLogicalMinExpr(b, ConstantOf(other))
}

func (b *baseExpr) Eq(other Expr) *EqCondition { return newEqCondition(b, other) }
func (b *baseExpr) EqValue(other any) *EqCondition {
	return newEqCondition(b, ConstantOf(other))
}
func (b *baseExpr) Neq(other Expr) *NeqCondition { return newNeqCondition(b, other) }
func (b *baseExpr) NeqValue(other any) *NeqCondition {
	return newNeqCondition(b, ConstantOf(other))
}
func (b *baseExpr) Gt(other Expr) *GtCondition { return newGtCondition(b, other) }
func (b *baseExpr) GtValue(other any) *GtCondition {
	return newGtCondition(b, ConstantOf(other))
}
func (b *baseExpr) Gte(other Expr) *GteCondition { return newGteCondition(b, other) }
func (b *baseExpr) GteValue(other any) *GteCondition {
	return newGteCondition(b, ConstantOf(other))
}
func (b *baseExpr) Lt(other Expr) *LtCondition { return newLtCondition(b, other) }
func (b *baseExpr) LtValue(other any) *LtCondition {
	return newLtCondition(b, ConstantOf(other))
}
func (b *baseExpr) Lte(other Expr) *LteCondition { return newLteCondition(b, other) }
func (b *baseExpr) LteValue(other any) *LteCondition {
	return newLteCondition(b, ConstantOf(other))
}
func (b *baseExpr) InAny(values ...any) *InCondition {
	return newInCondition(b, toExprList(values...))
}
func (b *baseExpr) NotInAny(values ...any) *NotCondition {
	return newNotCondition(newInCondition(b, toExprList(values...)))
}
func (b *baseExpr) Exists() *ExistsCondition { return newExistsCondition(b) }
func (b *baseExpr) IsNaN() *IsNaNCondition   { return newIsNaNCondition(b) }

func (b *baseExpr) ArrayConcat(elements ...any) *ArrayConcatExpr {
	return newArrayConcatExpr(b, toExprList(elements...))
}
func (b *baseExpr) ArrayContains(element Expr) *ArrayContainsCondition {
	return newArrayContainsCondition(b, element)
}
func (b *baseExpr) ArrayContainsValue(element any) *ArrayContainsCondition {
	return newArrayContainsCondition(b, ConstantOf(element))
}
func (b *baseExpr) ArrayContainsAll(elements ...Expr) *ArrayContainsAllCondition {
	return newArrayContainsAllCondition(b, elements)
}
func (b *baseExpr) ArrayContainsAllValues(elements ...any) *ArrayContainsAllCondition {
	return newArrayContainsAllCondition(b, toExprList(elements...))
}
func (b *baseExpr) ArrayContainsAny(elements ...Expr) *ArrayContainsAnyCondition {
	return newArrayContainsAnyCondition(b, elements)
}
func (b *baseExpr) ArrayContainsAnyValues(elements ...any) *ArrayContainsAnyCondition {
	return newArrayContainsAnyCondition(b, toExprList(elements...))
}
func (b *baseExpr) ArrayLength() *ArrayLengthExpr { return newArrayLengthExpr(b) }
func (b *baseExpr) ArrayReverse() *ArrayReverseExpr {
	return newArrayReverseExpr(b)
}
func (b *baseExpr) ArrayFilter(condition FilterCondition) *ArrayFilterExpr {
	return newArrayFilterExpr(b, condition)
}
func (b *baseExpr) ArrayTransform(transform *Function) *ArrayTransformExpr {
	return newArrayTransformExpr(b, transform)
}
func (b *baseExpr) ArrayElement() *ArrayElementExpr {
	return newArrayElementExpr()
}

func (b *baseExpr) CharLength() *CharLengthExpr { return newCharLengthExpr(b) }
func (b *baseExpr) ByteLength() *ByteLengthExpr { return newByteLengthExpr(b) }
func (b *baseExpr) Like(pattern Expr) *LikeCondition {
	return newLikeCondition(b, pattern)
}
func (b *baseExpr) LikeString(pattern string) *LikeCondition {
	return newLikeCondition(b, ConstantOf(pattern))
}
func (b *baseExpr) RegexContains(regex Expr) *RegexContainsCondition {
	return newRegexContainsCondition(b, regex)
}
func (b *baseExpr) RegexContainsString(regex string) *RegexContainsCondition {
	return newRegexContainsCondition(b, ConstantOf(regex))
}
func (b *baseExpr) RegexMatches(regex Expr) *RegexMatchCondition {
	return newRegexMatchCondition(b, regex)
}
func (b *baseExpr) RegexMatchesString(regex string) *RegexMatchCondition {
	return newRegexMatchCondition(b, ConstantOf(regex))
}
func (b *baseExpr) StrContains(substring Expr) *StrContainsCondition {
	return newStrContainsCondition(b, substring)
}
func (b *baseExpr) StrContainsString(substring string) *StrContainsCondition {
	return newStrContainsCondition(b, ConstantOf(substring))
}
func (b *baseExpr) StartsWith(prefix Expr) *StartsWithCondition {
	return newStartsWithCondition(b, prefix)
}
func (b *baseExpr) StartsWithString(prefix string) *StartsWithCondition {
	return newStartsWithCondition(b, ConstantOf(prefix))
}
func (b *baseExpr) EndsWith(postfix Expr) *EndsWithCondition {
	return newEndsWithCondition(b, postfix)
}
func (b *baseExpr) EndsWithString(postfix string) *EndsWithCondition {
	return newEndsWithCondition(b, ConstantOf(postfix))
}
func (b *baseExpr) StrConcat(elements ...any) *StrConcatExpr {
	return newStrConcatExpr(b, toExprList(elements...))
}
func (b *baseExpr) ToLower() *ToLowerExpr { return newToLowerExpr(b) }
func (b *baseExpr) ToUpper() *ToUpperExpr { return newToUpperExpr(b) }
func (b *baseExpr) Trim() *TrimExpr       { return newTrimExpr(b) }
func (b *baseExpr) Reverse() *ReverseExpr { return newReverseExpr(b) }
func (b *baseExpr) ReplaceFirst(find, replace Expr) *ReplaceFirstExpr {
	return newReplaceFirstExpr(b, find, replace)
}
func (b *baseExpr) ReplaceFirstStrings(find, replace string) *ReplaceFirstExpr {
	return newReplaceFirstExpr(b, ConstantOf(find), ConstantOf(replace))
}
func (b *baseExpr) ReplaceAll(find, replace Expr) *ReplaceAllExpr {
	return newReplaceAllExpr(b, find, replace)
}
func (b *baseExpr) ReplaceAllStrings(find, replace string) *ReplaceAllExpr {
	return newReplaceAllExpr(b, ConstantOf(find), ConstantOf(replace))
}

func (b *baseExpr) MapGet(key string) *MapGetExpr { return newMapGetExpr(b, key) }

func (b *baseExpr) CosineDistance(other Expr) *CosineDistanceExpr {
	return newCosineDistanceExpr(b, other)
}
func (b *baseExpr) CosineDistanceVector(other []float64) *CosineDistanceExpr {
	return newCosineDistanceExpr(b, ConstantVector64(other))
}
func (b *baseExpr) EuclideanDistance(other Expr) *EuclideanDistanceExpr {
	return newEuclideanDistanceExpr(b, other)
}
func (b *baseExpr) EuclideanDistanceVector(other []float64) *EuclideanDistanceExpr {
	return newEuclideanDistanceExpr(b, ConstantVector64(other))
}
func (b *baseExpr) DotProduct(other Expr) *DotProductExpr {
	return newDotProductExpr(b, other)
}
func (b *baseExpr) DotProductVector(other []float64) *DotProductExpr {
	return newDotProductExpr(b, ConstantVector64(other))
}
func (b *baseExpr) VectorLength() *VectorLengthExpr { return newVectorLengthExpr(b) }

func (b *baseExpr) TimestampToUnixMicros() *TimestampToUnixMicrosExpr {
	return newTimestampToUnixMicrosExpr(b)
}
func (b *baseExpr) UnixMicrosToTimestamp() *UnixMicrosToTimestampExpr {
	return newUnixMicrosToTimestampExpr(b)
}
func (b *baseExpr) TimestampToUnixMillis() *TimestampToUnixMillisExpr {
	return newTimestampToUnixMillisExpr(b)
}
func (b *baseExpr) UnixMillisToTimestamp() *UnixMillisToTimestampExpr {
	return newUnixMillisToTimestampExpr(b)
}
func (b *baseExpr) TimestampToUnixSeconds() *TimestampToUnixSecondsExpr {
	return newTimestampToUnixSecondsExpr(b)
}
func (b *baseExpr) UnixSecondsToTimestamp() *UnixSecondsToTimestampExpr {
	return newUnixSecondsToTimestampExpr(b)
}
func (b *baseExpr) TimestampAdd(unit Expr, amount Expr) *TimestampAddExpr {
	return newTimestampAddExpr(b, unit, amount)
}
func (b *baseExpr) TimestampAddUnitAmount(unit string, amount float64) *TimestampAddExpr {
	return newTimestampAddExpr(b, ConstantOf(unit), ConstantOf(amount))
}
func (b *baseExpr) TimestampSub(unit Expr, amount Expr) *TimestampSubExpr {
	return newTimestampSubExpr(b, unit, amount)
}
func (b *baseExpr) TimestampSubUnitAmount(unit string, amount float64) *TimestampSubExpr {
	return newTimestampSubExpr(b, ConstantOf(unit), ConstantOf(amount))
}

func (b *baseExpr) CollectionID() *CollectionIDExpr { return newCollectionIDExpr(b) }
func (b *baseExpr) Parent() *ParentExpr             { return newParentExpr(b) }

func (b *baseExpr) Ascending() *Ordering  { return newOrdering(b, DirectionAscending) }
func (b *baseExpr) Descending() *Ordering { return newOrdering(b, DirectionDescending) }

func (b *baseExpr) Sum() *SumAccumulator     { return newSumAccumulator(b, false) }
func (b *baseExpr) Avg() *AvgAccumulator     { return newAvgAccumulator(b, false) }
func (b *baseExpr) Count() *CountAccumulator { return newCountAccumulator(b) }
func (b *baseExpr) Min() *MinAccumulator     { return newMinAccumulator(b, false) }
func (b *baseExpr) Max() *MaxAccumulator     { return newMaxAccumulator(b, false) }

// Selectable is an interface for expressions that can be selected in a pipeline.
type Selectable interface {
	// getSelectionDetails returns the output alias and the underlying expression.
	getSelectionDetails() (alias string, expr Expr, err error)
}

// Constant represents a constant value in a pipeline expression.
type Constant struct {
	baseExpr
	value *pb.Value
	err   error
}

// ConstantOf creates a new Constant expression from a Go value.
func ConstantOf(value any) *Constant {
	c, err := constantOf(value)
	if err != nil {
		return &Constant{err: err}
	}
	return c
}

func constantOf(val any) (*Constant, error) {
	protoVal, _, err := toProtoValue(reflect.ValueOf(val))
	if err != nil {
		return nil, err
	}
	return (&Constant{value: protoVal}), nil
}

// ConstantNull creates a new Constant expression representing a null value.
func ConstantNull() *Constant {
	return &Constant{value: nil}
}

// ConstantVector creates a new Constant expression from a slice of doubles.
func ConstantVector32(value []float32) *Constant {
	return &Constant{value: vectorToProtoValue(value)}
}

// ConstantVector creates a new Constant expression from a slice of doubles.
func ConstantVector64(value []float64) *Constant {
	return &Constant{value: vectorToProtoValue(value)}
}

func (c *Constant) toProto() (*pb.Value, error) {
	return encodeValue(c.value)
}

func (c *Constant) As(alias string) Selectable {
	return newExprWithAlias(c, alias)
}

// Field represents a document field in a pipeline expression.
type Field struct {
	baseExpr
	path FieldPath
}

// DOCUMENT_ID is a special field path for the document ID.
const DOCUMENT_ID = "__name__"

// FieldOf creates a new Field expression from a field path string.
func FieldOf(path string) *Field {
	return &Field{path: FieldPath(strings.Split(path, "."))}
}

func (f *Field) toProto() (*pb.Value, error) {
	return &pb.Value{
		ValueType: &pb.Value_FieldReferenceValue{
			FieldReferenceValue: f.path.toServiceFieldPath(),
		},
	}, nil
}

func (f *Field) As(alias string) Selectable {
	return newExprWithAlias(f, alias)
}

func (f *Field) getSelectionDetails() (string, Expr, error) {
	return f.path.toServiceFieldPath(), f, nil
}

// ExprWithAlias represents an expression with an alias.
type ExprWithAlias struct {
	baseExpr
	alias string
	err   error
	expr  Expr
}

func newExprWithAlias(expr Expr, alias string) *ExprWithAlias {
	return &ExprWithAlias{expr: expr, alias: alias}
}

func (e *ExprWithAlias) toProto() (*pb.Value, error) {
	exprProto, err := e.expr.toProto()
	if err != nil {
		return nil, err
	}
	return exprProto, nil
}

func (e *ExprWithAlias) As(alias string) Selectable {
	return newExprWithAlias(e.expr, alias)
}

func (ewa *ExprWithAlias) getSelectionDetails() (string, Expr, error) {
	if ewa.err != nil {
		return "", nil, ewa.err
	}
	return ewa.alias, ewa.expr, nil
}

// GetAlias returns the alias of the expression.
func (e *ExprWithAlias) GetAlias() string {
	return e.alias
}

// GetExpr returns the underlying expression.
func (e *ExprWithAlias) GetExpr() Expr {
	return e.expr
}

// Function is a concrete struct that represents a function in a pipeline expression.
type Function struct {
	baseExpr
	name   string
	params []Expr
	err    error
}

func newFunction(name string, params ...Expr) *Function {
	return &Function{name: name, params: params}
}

func (f *Function) toProto() (*pb.Value, error) {
	if f.err != nil {
		return nil, f.err
	}
	argProtos := make([]*pb.Value, len(f.params))
	for i, arg := range f.params {
		var err error
		argProtos[i], err = arg.toProto()
		if err != nil {
			return nil, fmt.Errorf("error converting arg %d for function %q: %w", i, f.name, err)
		}
	}
	return &pb.Value{ValueType: &pb.Value_FunctionValue{
		FunctionValue: &pb.Function{
			Name: f.name,
			Args: argProtos,
		},
	}}, nil
}

func (f *Function) As(alias string) Selectable {
	return newExprWithAlias(f, alias)
}

// FilterCondition is an interface that represents a filter condition in a pipeline expression.
type FilterCondition interface {
	Expr // Embed Expr interface
	isFilterCondition()
}

// baseFilterCondition provides common methods for all FilterCondition implementations.
type baseFilterCondition struct {
	Function // Embed Function to get Expr methods and toProto
}

func (b *baseFilterCondition) isFilterCondition() {}

// Accumulator is an interface that represents an aggregation function in a pipeline.
type Accumulator interface {
	Expr // Embed Expr interface
	isAccumulator()
	As(alias string) *ExprWithAlias // Accumulators can also be aliased
}

// baseAccumulator provides common methods for all Accumulator implementations.
type baseAccumulator struct {
	Function // Embed Function to get Expr methods and toProto
}

func (b *baseAccumulator) isAccumulator() {}
func (b *baseAccumulator) As(alias string) *ExprWithAlias {
	return newExprWithAlias(b, alias)
}

// Concrete Expression Implementations (Arithmetic)

type AddExpr struct {
	Function
}

func newAddExpr(left, right Expr) *AddExpr {
	return &AddExpr{Function: *newFunction("add", left, right)}
}

type SubtractExpr struct {
	Function
}

func newSubtractExpr(left, right Expr) *SubtractExpr {
	return &SubtractExpr{Function: *newFunction("subtract", left, right)}
}

type MultiplyExpr struct {
	Function
}

func newMultiplyExpr(left, right Expr) *MultiplyExpr {
	return &MultiplyExpr{Function: *newFunction("multiply", left, right)}
}

type DivideExpr struct {
	Function
}

func newDivideExpr(left, right Expr) *DivideExpr {
	return &DivideExpr{Function: *newFunction("divide", left, right)}
}

type ModExpr struct {
	Function
}

func newModExpr(left, right Expr) *ModExpr {
	return &ModExpr{Function: *newFunction("mod", left, right)}
}

type LogicalMaxExpr struct {
	Function
}

func newLogicalMaxExpr(left, right Expr) *LogicalMaxExpr {
	return &LogicalMaxExpr{Function: *newFunction("logical_max", left, right)}
}

type LogicalMinExpr struct {
	Function
}

func newLogicalMinExpr(left, right Expr) *LogicalMinExpr {
	return &LogicalMinExpr{Function: *newFunction("logical_min", left, right)}
}

// Concrete Expression Implementations (Comparison/Filter)

type EqCondition struct {
	baseFilterCondition
}

func newEqCondition(left, right Expr) *EqCondition {
	return &EqCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("eq", left, right)}}
}

type NeqCondition struct {
	baseFilterCondition
}

func newNeqCondition(left, right Expr) *NeqCondition {
	return &NeqCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("neq", left, right)}}
}

type GtCondition struct {
	baseFilterCondition
}

func newGtCondition(left, right Expr) *GtCondition {
	return &GtCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("gt", left, right)}}
}

type GteCondition struct {
	baseFilterCondition
}

func newGteCondition(left, right Expr) *GteCondition {
	return &GteCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("gte", left, right)}}
}

type LtCondition struct {
	baseFilterCondition
}

func newLtCondition(left, right Expr) *LtCondition {
	return &LtCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("lt", left, right)}}
}

type LteCondition struct {
	baseFilterCondition
}

func newLteCondition(left, right Expr) *LteCondition {
	return &LteCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("lte", left, right)}}
}

type InCondition struct {
	baseFilterCondition
}

func newInCondition(left Expr, others []Expr) *InCondition {
	return &InCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("in", left, newListOfExprs(others))}}
}

type NotCondition struct {
	baseFilterCondition
}

func newNotCondition(condition FilterCondition) *NotCondition {
	return &NotCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("not", condition)}}
}

type AndCondition struct {
	baseFilterCondition
}

func newAndCondition(conditions ...FilterCondition) *AndCondition {
	exprs := make([]Expr, len(conditions))
	for i, c := range conditions {
		exprs[i] = c
	}
	return &AndCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("and", exprs...)}}
}

type OrCondition struct {
	baseFilterCondition
}

func newOrCondition(conditions ...FilterCondition) *OrCondition {
	exprs := make([]Expr, len(conditions))
	for i, c := range conditions {
		exprs[i] = c
	}
	return &OrCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("or", exprs...)}}
}

type XorCondition struct {
	baseFilterCondition
}

func newXorCondition(conditions ...FilterCondition) *XorCondition {
	exprs := make([]Expr, len(conditions))
	for i, c := range conditions {
		exprs[i] = c
	}
	return &XorCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("xor", exprs...)}}
}

type IfCondition struct {
	baseFilterCondition
}

func newIfCondition(condition FilterCondition, thenExpr Expr, elseExpr Expr) *IfCondition {
	params := []Expr{condition, thenExpr}
	if elseExpr != nil {
		params = append(params, elseExpr)
	}
	return &IfCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("if", params...)}}
}

type ExistsCondition struct {
	baseFilterCondition
}

func newExistsCondition(expr Expr) *ExistsCondition {
	return &ExistsCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("exists", expr)}}
}

type IsNaNCondition struct {
	baseFilterCondition
}

func newIsNaNCondition(value Expr) *IsNaNCondition {
	return &IsNaNCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("is_nan", value)}}
}

// Concrete Expression Implementations (Array Functions)

type ArrayConcatExpr struct {
	Function
}

func newArrayConcatExpr(array Expr, rest []Expr) *ArrayConcatExpr {
	params := append([]Expr{array}, rest...)
	return &ArrayConcatExpr{Function: *newFunction("array_concat", params...)}
}

type ArrayContainsCondition struct {
	baseFilterCondition
}

func newArrayContainsCondition(array, element Expr) *ArrayContainsCondition {
	return &ArrayContainsCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("array_contains", array, element)}}
}

type ArrayContainsAllCondition struct {
	baseFilterCondition
}

func newArrayContainsAllCondition(array Expr, elements []Expr) *ArrayContainsAllCondition {
	return &ArrayContainsAllCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("array_contains_all", array, newListOfExprs(elements))}}
}

type ArrayContainsAnyCondition struct {
	baseFilterCondition
}

func newArrayContainsAnyCondition(array Expr, elements []Expr) *ArrayContainsAnyCondition {
	return &ArrayContainsAnyCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("array_contains_any", array, newListOfExprs(elements))}}
}

type ArrayLengthExpr struct {
	Function
}

func newArrayLengthExpr(array Expr) *ArrayLengthExpr {
	return &ArrayLengthExpr{Function: *newFunction("array_length", array)}
}

type ArrayReverseExpr struct {
	Function
}

func newArrayReverseExpr(array Expr) *ArrayReverseExpr {
	return &ArrayReverseExpr{Function: *newFunction("array_reverse", array)}
}

type ArrayFilterExpr struct {
	Function
}

func newArrayFilterExpr(array Expr, filter FilterCondition) *ArrayFilterExpr {
	return &ArrayFilterExpr{Function: *newFunction("array_filter", array, filter)}
}

type ArrayTransformExpr struct {
	Function
}

func newArrayTransformExpr(array Expr, transform *Function) *ArrayTransformExpr {
	return &ArrayTransformExpr{Function: *newFunction("array_transform", array, transform)}
}

type ArrayElementExpr struct {
	Function
}

func newArrayElementExpr() *ArrayElementExpr {
	return &ArrayElementExpr{Function: *newFunction("array_element")}
}

// Concrete Expression Implementations (String Functions)

type ByteLengthExpr struct {
	Function
}

func newByteLengthExpr(expr Expr) *ByteLengthExpr {
	return &ByteLengthExpr{Function: *newFunction("byte_length", expr)}
}

type CharLengthExpr struct {
	Function
}

func newCharLengthExpr(expr Expr) *CharLengthExpr {
	return &CharLengthExpr{Function: *newFunction("char_length", expr)}
}

type EndsWithCondition struct {
	baseFilterCondition
}

func newEndsWithCondition(expr, postfix Expr) *EndsWithCondition {
	return &EndsWithCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("ends_with", expr, postfix)}}
}

type LikeCondition struct {
	baseFilterCondition
}

func newLikeCondition(expr, pattern Expr) *LikeCondition {
	return &LikeCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("like", expr, pattern)}}
}

type RegexContainsCondition struct {
	baseFilterCondition
}

func newRegexContainsCondition(expr, regex Expr) *RegexContainsCondition {
	return &RegexContainsCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("regex_contains", expr, regex)}}
}

type RegexMatchCondition struct {
	baseFilterCondition
}

func newRegexMatchCondition(expr, regex Expr) *RegexMatchCondition {
	return &RegexMatchCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("regex_match", expr, regex)}}
}

type ReplaceAllExpr struct {
	Function
}

func newReplaceAllExpr(value, find, replacement Expr) *ReplaceAllExpr {
	return &ReplaceAllExpr{Function: *newFunction("replace_all", value, find, replacement)}
}

type ReplaceFirstExpr struct {
	Function
}

func newReplaceFirstExpr(value, find, replacement Expr) *ReplaceFirstExpr {
	return &ReplaceFirstExpr{Function: *newFunction("replace_first", value, find, replacement)}
}

type ReverseExpr struct {
	Function
}

func newReverseExpr(expr Expr) *ReverseExpr {
	return &ReverseExpr{Function: *newFunction("reverse", expr)}
}

type StartsWithCondition struct {
	baseFilterCondition
}

func newStartsWithCondition(expr, prefix Expr) *StartsWithCondition {
	return &StartsWithCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("starts_with", expr, prefix)}}
}

type StrConcatExpr struct {
	Function
}

func newStrConcatExpr(first Expr, exprs []Expr) *StrConcatExpr {
	params := append([]Expr{first}, exprs...)
	return &StrConcatExpr{Function: *newFunction("str_concat", params...)}
}

type StrContainsCondition struct {
	baseFilterCondition
}

func newStrContainsCondition(expr, substring Expr) *StrContainsCondition {
	return &StrContainsCondition{baseFilterCondition: baseFilterCondition{Function: *newFunction("str_contains", expr, substring)}}
}

type ToLowerExpr struct {
	Function
}

func newToLowerExpr(expr Expr) *ToLowerExpr {
	return &ToLowerExpr{Function: *newFunction("to_lower", expr)}
}

type ToUpperExpr struct {
	Function
}

func newToUpperExpr(expr Expr) *ToUpperExpr {
	return &ToUpperExpr{Function: *newFunction("to_upper", expr)}
}

type TrimExpr struct {
	Function
}

func newTrimExpr(expr Expr) *TrimExpr {
	return &TrimExpr{Function: *newFunction("trim", expr)}
}

// Concrete Expression Implementations (Map Functions)

type MapGetExpr struct {
	Function
}

func newMapGetExpr(m Expr, key string) *MapGetExpr {
	return &MapGetExpr{Function: *newFunction("map_get", m, ConstantOf(key))}
}

// Concrete Expression Implementations (Vector Functions)

type CosineDistanceExpr struct {
	Function
}

func newCosineDistanceExpr(vector1, vector2 Expr) *CosineDistanceExpr {
	return &CosineDistanceExpr{Function: *newFunction("cosine_distance", vector1, vector2)}
}

type DotProductExpr struct {
	Function
}

func newDotProductExpr(vector1, vector2 Expr) *DotProductExpr {
	return &DotProductExpr{Function: *newFunction("dot_product", vector1, vector2)}
}

type EuclideanDistanceExpr struct {
	Function
}

func newEuclideanDistanceExpr(vector1, vector2 Expr) *EuclideanDistanceExpr {
	return &EuclideanDistanceExpr{Function: *newFunction("euclidean_distance", vector1, vector2)}
}

type VectorLengthExpr struct {
	Function
}

func newVectorLengthExpr(array Expr) *VectorLengthExpr {
	return &VectorLengthExpr{Function: *newFunction("vector_length", array)}
}

// Concrete Expression Implementations (Timestamp Functions)

type TimestampAddExpr struct {
	Function
}

func newTimestampAddExpr(timestamp, unit, amount Expr) *TimestampAddExpr {
	return &TimestampAddExpr{Function: *newFunction("timestamp_add", timestamp, unit, amount)}
}

type TimestampSubExpr struct {
	Function
}

func newTimestampSubExpr(timestamp, unit, amount Expr) *TimestampSubExpr {
	return &TimestampSubExpr{Function: *newFunction("timestamp_sub", timestamp, unit, amount)}
}

type TimestampToUnixMicrosExpr struct {
	Function
}

func newTimestampToUnixMicrosExpr(input Expr) *TimestampToUnixMicrosExpr {
	return &TimestampToUnixMicrosExpr{Function: *newFunction("timestamp_to_unix_micros", input)}
}

type TimestampToUnixMillisExpr struct {
	Function
}

func newTimestampToUnixMillisExpr(input Expr) *TimestampToUnixMillisExpr {
	return &TimestampToUnixMillisExpr{Function: *newFunction("timestamp_to_unix_millis", input)}
}

type TimestampToUnixSecondsExpr struct {
	Function
}

func newTimestampToUnixSecondsExpr(input Expr) *TimestampToUnixSecondsExpr {
	return &TimestampToUnixSecondsExpr{Function: *newFunction("timestamp_to_unix_seconds", input)}
}

type UnixMicrosToTimestampExpr struct {
	Function
}

func newUnixMicrosToTimestampExpr(input Expr) *UnixMicrosToTimestampExpr {
	return &UnixMicrosToTimestampExpr{Function: *newFunction("unix_micros_to_timestamp", input)}
}

type UnixMillisToTimestampExpr struct {
	Function
}

func newUnixMillisToTimestampExpr(input Expr) *UnixMillisToTimestampExpr {
	return &UnixMillisToTimestampExpr{Function: *newFunction("unix_millis_to_timestamp", input)}
}

type UnixSecondsToTimestampExpr struct {
	Function
}

func newUnixSecondsToTimestampExpr(input Expr) *UnixSecondsToTimestampExpr {
	return &UnixSecondsToTimestampExpr{Function: *newFunction("unix_seconds_to_timestamp", input)}
}

// Concrete Expression Implementations (Document Functions)

type CollectionIDExpr struct {
	Function
}

func newCollectionIDExpr(value Expr) *CollectionIDExpr {
	return &CollectionIDExpr{Function: *newFunction("collection_id", value)}
}

type ParentExpr struct {
	Function
}

func newParentExpr(value Expr) *ParentExpr {
	return &ParentExpr{Function: *newFunction("parent", value)}
}

// Concrete Accumulator Implementations

type AvgAccumulator struct {
	baseAccumulator
}

func newAvgAccumulator(value Expr, distinct bool) *AvgAccumulator {
	if distinct {
		return &AvgAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("avg_distinct", value)}}
	}
	return &AvgAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("avg", value)}}
}

type CountAccumulator struct {
	baseAccumulator
}

func newCountAccumulator(value Expr) *CountAccumulator {
	return &CountAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("count", value)}}
}

// CountAll creates a CountAccumulator that counts all documents.
func CountAll() *CountAccumulator {
	return &CountAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("count_all")}}
}

type CountIfAccumulator struct {
	baseAccumulator
}

func newCountIfAccumulator(condition FilterCondition, distinct bool) *CountIfAccumulator {
	if distinct {
		return &CountIfAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("count_if_distinct", condition)}}
	}
	return &CountIfAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("count_if", condition)}}
}

type MaxAccumulator struct {
	baseAccumulator
}

func newMaxAccumulator(value Expr, distinct bool) *MaxAccumulator {
	if distinct {
		return &MaxAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("max_distinct", value)}}
	}
	return &MaxAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("max", value)}}
}

type MinAccumulator struct {
	baseAccumulator
}

func newMinAccumulator(value Expr, distinct bool) *MinAccumulator {
	if distinct {
		return &MinAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("min_distinct", value)}}
	}
	return &MinAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("min", value)}}
}

type SumAccumulator struct {
	baseAccumulator
}

func newSumAccumulator(value Expr, distinct bool) *SumAccumulator {
	if distinct {
		return &SumAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("sum_distinct", value)}}
	}
	return &SumAccumulator{baseAccumulator: baseAccumulator{Function: *newFunction("sum", value)}}
}

// ListOfExprs is an internal helper for representing a list of expressions as a single Expr.
type listOfExprs struct {
	baseExpr
	conditions []Expr
}

func newListOfExprs(list []Expr) *listOfExprs {
	return &listOfExprs{conditions: list}
}

func (l *listOfExprs) toProto() (*pb.Value, error) {
	protoExprs := make([]*pb.Value, len(l.conditions))
	for i, e := range l.conditions {
		pv, err := e.toProto()
		if err != nil {
			return nil, err
		}
		protoExprs[i] = pv
	}
	return &pb.Value{
		ValueType: &pb.Value_ArrayValue{
			ArrayValue: &pb.ArrayValue{
				Values: protoExprs,
			},
		},
	}, nil
}

// GetConditions returns the underlying list of expressions.
func (l *listOfExprs) GetConditions() []Expr {
	return l.conditions
}

// Ordering represents a field and its direction for sorting.
type Ordering struct {
	expr Expr
	dir  OrderingDirection
}

// OrderingDirection represents the sort direction.
type OrderingDirection int

const (
	DirectionAscending OrderingDirection = iota
	DirectionDescending
)

func (d OrderingDirection) String() string {
	switch d {
	case DirectionAscending:
		return "ASCENDING"
	case DirectionDescending:
		return "DESCENDING"
	default:
		return "UNKNOWN"
	}
}

func newOrdering(expr Expr, dir OrderingDirection) *Ordering {
	return &Ordering{expr: expr, dir: dir}
}

// Ascending creates an ascending order for the given expression.
func Ascending(expr Expr) *Ordering {
	return newOrdering(expr, DirectionAscending)
}

// Descending creates a descending order for the given expression.
func Descending(expr Expr) *Ordering {
	return newOrdering(expr, DirectionDescending)
}

func (o *Ordering) toProto() (*pb.Value, error) {
	exprProto, err := o.expr.toProto()
	if err != nil {
		return nil, err
	}

	dirStr := ""
	switch o.dir {
	case DirectionAscending:
		dirStr = "ASCENDING"
	case DirectionDescending:
		dirStr = "DESCENDING"
	default:
		return nil, fmt.Errorf("firestore: unknown ordering direction %v", o.dir)
	}

	return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: map[string]*pb.Value{
		"direction":  stringToProtoValue(dirStr),
		"expression": exprProto,
	}}}}, nil
}

// GetExpr returns the expression being ordered.
func (o *Ordering) GetExpr() Expr {
	return o.expr
}

// GetDir returns the direction of the ordering.
func (o *Ordering) GetDir() OrderingDirection {
	return o.dir
}

// pipeline_stages.go

// Stage is an interface that represents a stage in a Firestore pipeline.
type Stage interface {
	toStageProto() (*pb.Pipeline_Stage, error)
	name() string // For identification, logging, and potential validation
}

// CollectionStage returns all documents from the entire collection.
type CollectionStage struct {
	path string
}

func newCollectionStage(path string) *CollectionStage {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return &CollectionStage{path: path}
}
func (s *CollectionStage) name() string { return "collection" }
func (s *CollectionStage) toStageProto() (*pb.Pipeline_Stage, error) {
	arg := &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: s.path}}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{arg},
	}, nil
}

// CollectionGroupStage returns all documents from a collection group.
type CollectionGroupStage struct {
	collectionID string
	ancestor     string
}

func newCollectionGroupStage(ancestor, collectionID string) *CollectionGroupStage {
	return &CollectionGroupStage{ancestor: ancestor, collectionID: collectionID}
}
func (s *CollectionGroupStage) name() string { return "collection_group" }
func (s *CollectionGroupStage) toStageProto() (*pb.Pipeline_Stage, error) {
	ancestor := &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: s.ancestor}}
	collectionID := &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s.collectionID}}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{ancestor, collectionID},
	}, nil
}

// DatabaseStage returns all documents from the entire database.
type DatabaseStage struct{}

func newDatabaseStage() *DatabaseStage { return &DatabaseStage{} }
func (s *DatabaseStage) name() string  { return "database" }
func (s *DatabaseStage) toStageProto() (*pb.Pipeline_Stage, error) {
	return &pb.Pipeline_Stage{
		Name: s.name(),
	}, nil
}

// DocumentsStage operates on a specific set of documents.
type DocumentsStage struct {
	documents []string
}

func newDocumentsStage(docs ...*DocumentRef) *DocumentsStage {
	docPaths := make([]string, len(docs))
	for i, doc := range docs {
		docPaths[i] = doc.Path
	}
	return &DocumentsStage{documents: docPaths}
}
func (s *DocumentsStage) name() string { return "documents" }
func (s *DocumentsStage) toStageProto() (*pb.Pipeline_Stage, error) {
	protoDocs := make([]*pb.Value, len(s.documents))
	for i, docPath := range s.documents {
		protoDocs[i] = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: docPath}}
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: protoDocs,
	}, nil
}

// AddFieldsStage adds new fields to the documents in the pipeline.
type AddFieldsStage struct {
	args []*pb.Value
}

func newAddFieldsStage(selectables ...Selectable) (*AddFieldsStage, error) {
	args, err := selectablesToPbVals(selectables...)
	if err != nil {
		return nil, err
	}
	return &AddFieldsStage{args: args}, nil
}
func (s *AddFieldsStage) name() string { return "add_fields" }
func (s *AddFieldsStage) toStageProto() (*pb.Pipeline_Stage, error) {
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: s.args,
	}, nil
}

// RemoveFieldsStage removes fields from the documents in the pipeline.
type RemoveFieldsStage struct {
	args []*pb.Value
}

func newRemoveFieldsStage(fields ...Field) (*RemoveFieldsStage, error) {
	selectables := make([]Selectable, len(fields))
	for i, f := range fields {
		selectables[i] = &f
	}

	args, err := selectablesToPbVals(selectables...)
	if err != nil {
		return nil, err
	}
	return &RemoveFieldsStage{args: args}, nil
}
func (s *RemoveFieldsStage) name() string { return "remove_fields" }
func (s *RemoveFieldsStage) toStageProto() (*pb.Pipeline_Stage, error) {
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: s.args,
	}, nil
}

// SelectStage selects specific fields from the documents in the pipeline.
type SelectStage struct {
	args []*pb.Value
}

func newSelectStage(selectables ...Selectable) (*SelectStage, error) {
	args, err := selectablesToPbVals(selectables...)
	if err != nil {
		return nil, err
	}
	return &SelectStage{args: args}, nil
}
func (s *SelectStage) name() string { return "select" }
func (s *SelectStage) toStageProto() (*pb.Pipeline_Stage, error) {
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: s.args,
	}, nil
}

// WhereStage filters documents based on a condition.
type WhereStage struct {
	condition FilterCondition
}

func newWhereStage(condition FilterCondition) *WhereStage {
	return &WhereStage{condition: condition}
}
func (s *WhereStage) name() string { return "where" }
func (s *WhereStage) toStageProto() (*pb.Pipeline_Stage, error) {
	protoCondition, err := s.condition.toProto()
	if err != nil {
		return nil, err
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{protoCondition},
	}, nil
}

// OffsetStage skips a specified number of documents.
type OffsetStage struct {
	offset int
}

func newOffsetStage(offset int) *OffsetStage {
	return &OffsetStage{offset: offset}
}
func (s *OffsetStage) name() string { return "offset" }
func (s *OffsetStage) toStageProto() (*pb.Pipeline_Stage, error) {
	arg := &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(s.offset)}}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{arg},
	}, nil
}

// AggregateStage performs aggregation operations.
type AggregateStage struct {
	groupsPbVal       *pb.Value
	accumulatorsPbVal *pb.Value
	err               error
}

func newAggregateStage(fields []Selectable, accumulators []*ExprWithAlias) *AggregateStage {
	as := &AggregateStage{}
	as.WithGroups(fields...)
	as.WithAccumulators(accumulators...)
	return as
}

// WithGroups sets the grouping fields for the aggregation.
func (s *AggregateStage) WithGroups(fields ...Selectable) *AggregateStage {
	groupsPbVal, err := selectablesToPbVal(fields...)
	if err != nil {
		return &AggregateStage{err: err}
	}
	s.groupsPbVal = groupsPbVal
	return s
}

// WithAccumulators sets the accumulators for the aggregation.
func (s *AggregateStage) WithAccumulators(accumulators ...*ExprWithAlias) *AggregateStage {
	if accumulators == nil || len(accumulators) == 0 {
		return s
	}
	ss := make([]Selectable, len(accumulators))
	accums := make(map[string]Accumulator)
	for i, ea := range accumulators {
		if acc, ok := ea.GetExpr().(Accumulator); ok {
			accums[ea.GetAlias()] = acc
			ss[i] = ea
		} else {
			// Error: not an accumulator
			// TODO: Think more about this
			s.err = fmt.Errorf("firestore: %v is not an accumulator", ea)
			return nil // Or return an error
		}
	}
	var err error
	s.accumulatorsPbVal, err = selectablesToPbVal(ss...)
	if err != nil {
		s.err = err
	}
	return s
}

func (s *AggregateStage) name() string { return "aggregate" }
func (s *AggregateStage) toStageProto() (*pb.Pipeline_Stage, error) {
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{
			s.groupsPbVal,
			s.accumulatorsPbVal,
		},
	}, nil
}

// DistinctStage returns only unique documents based on specified fields.
type DistinctStage struct {
	groups []Selectable
}

func newDistinctStage(groups ...Selectable) *DistinctStage {
	return &DistinctStage{groups: groups}
}
func (s *DistinctStage) name() string { return "distinct" }
func (s *DistinctStage) toStageProto() (*pb.Pipeline_Stage, error) {
	protoGroups, err := selectablesToPbVals(s.groups...)
	if err != nil {
		return nil, err
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: protoGroups,
	}, nil
}

type findNearestSetting struct {
	limit         *uint64
	distanceField *Field
}

type FindNearestStageOptions interface {
	Apply(s *findNearestSetting)
}

type withLimit struct{ limit uint64 }

func (w withLimit) Apply(s *findNearestSetting) {
	s.limit = &w.limit
}

func WithLimit(limit uint64) FindNearestStageOptions {
	return withLimit{limit: limit}
}

type withDistanceField struct{ distanceField Field }

func (w withDistanceField) Apply(s *findNearestSetting) {
	s.distanceField = &w.distanceField
}
func WithDistanceField(distanceField Field) FindNearestStageOptions {
	return withDistanceField{distanceField: distanceField}
}

type FindNearestStage struct {
	property        Expr
	vector          []float64
	distanceMeasure DistanceMeasure
	options         []FindNearestStageOptions
}

func newFindNearestStage(property Expr, vector []float64, distanceMeasure DistanceMeasure, options ...FindNearestStageOptions) *FindNearestStage {
	return &FindNearestStage{
		property:        property,
		vector:          vector,
		distanceMeasure: distanceMeasure,
		options:         options,
	}
}
func (s *FindNearestStage) name() string { return "find_nearest" }
func (s *FindNearestStage) toStageProto() (*pb.Pipeline_Stage, error) {
	propertyProto, err := s.property.toProto()
	if err != nil {
		return nil, err
	}
	vectorProto, err := ConstantVector64(s.vector).toProto()
	if err != nil {
		return nil, err
	}

	fns := &findNearestSetting{}
	for _, o := range s.options {
		o.Apply(fns)
	}

	dmStr := ""
	switch s.distanceMeasure {
	case DistanceMeasureEuclidean:
		dmStr = "euclidean"
	case DistanceMeasureCosine:
		dmStr = "cosine"
	case DistanceMeasureDotProduct:
		dmStr = "dot_product"
	default:
		return nil, fmt.Errorf("firestore: unknown distance measure %v", s.distanceMeasure)
	}

	args := []*pb.Value{propertyProto, vectorProto, stringToProtoValue(dmStr)}

	optionsMap := make(map[string]*pb.Value)
	if s.options != nil {

		if fns.limit != nil {
			optionsMap["limit"], _, err = toProtoValue(reflect.ValueOf(*fns.limit))
			if err != nil {
				return nil, err
			}
		}
		if fns.distanceField != nil {
			dfProto, err := fns.distanceField.toProto()
			if err != nil {
				return nil, err
			}
			optionsMap["distance_field"] = dfProto
		}
		args = append(args, &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: optionsMap}}})
	}

	return &pb.Pipeline_Stage{
		Name:    s.name(),
		Args:    args,
		Options: optionsMap,
	}, nil
}

// EuclideanDistanceMeasure implements DistanceMeasure for Euclidean distance.
type EuclideanDistanceMeasure struct{}

// Euclidean creates a EuclideanDistanceMeasure.
func Euclidean() DistanceMeasure                          { return DistanceMeasureEuclidean }
func (m *EuclideanDistanceMeasure) toProtoString() string { return "EUCLIDEAN" }

// CosineDistanceMeasure implements DistanceMeasure for Cosine distance.
type CosineDistanceMeasure struct{}

// Cosine creates a CosineDistanceMeasure.
func Cosine() DistanceMeasure                          { return DistanceMeasureCosine }
func (m *CosineDistanceMeasure) toProtoString() string { return "COSINE" }

// DotProductDistanceMeasure implements DistanceMeasure for Dot Product distance.
type DotProductDistanceMeasure struct{}

// DotProduct creates a DotProductDistanceMeasure.
func DotProduct() DistanceMeasure                          { return DistanceMeasureDotProduct }
func (m *DotProductDistanceMeasure) toProtoString() string { return "DOT_PRODUCT" }

// GenericDistanceMeasure implements DistanceMeasure for a custom distance measure.
type GenericDistanceMeasure struct {
	name string
}

// GenericStage represents a generic pipeline stage.
type GenericStage struct {
	name_  string
	params []any
}

func newGenericStage(name string, params ...any) *GenericStage {
	return &GenericStage{name_: name, params: params}
}
func (s *GenericStage) name() string { return s.name_ }
func (s *GenericStage) toStageProto() (*pb.Pipeline_Stage, error) {
	protoParams := make([]*pb.Value, len(s.params))
	for i, p := range s.params {
		pv, err := encodeValue(p)
		if err != nil {
			return nil, err
		}
		protoParams[i] = pv
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: protoParams,
	}, nil
}

// ReplaceStage replaces documents or merges fields.
type ReplaceStage struct {
	field Selectable
	mode  ReplaceMode
}

// ReplaceMode defines how documents are replaced or merged.
type ReplaceMode string

const (
	// FullReplace replaces the entire document.
	FullReplace ReplaceMode = "full_replace"
	// MergePreferNext merges fields, preferring values from the next document.
	MergePreferNext ReplaceMode = "merge_prefer_next"
	// MergePreferParent merges fields, preferring values from the parent document.
	MergePreferParent ReplaceMode = "merge_prefer_parent"
)

func newReplaceStage(field Selectable, mode ReplaceMode) *ReplaceStage {
	if mode == "" {
		mode = FullReplace // Default mode
	}
	return &ReplaceStage{field: field, mode: mode}
}
func (s *ReplaceStage) name() string { return "replace" }
func (s *ReplaceStage) toStageProto() (*pb.Pipeline_Stage, error) {
	_, fieldProto, err := selectableToPbVal(s.field)
	if err != nil {
		return nil, err
	}
	modeProto := stringToProtoValue(string(s.mode))
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{fieldProto, modeProto},
	}, nil
}

// SampleStage samples documents from the pipeline.
type SampleStage struct {
	options *SampleOptions
}

// SampleOptions provides options for the Sample stage.
type SampleOptions struct {
	n    any // Can be int (documents) or float64 (percent)
	mode SampleMode
}

// SampleMode defines the sampling mode.
type SampleMode string

const (
	// SampleDocuments samples a fixed number of documents.
	SampleDocuments SampleMode = "documents"
	// SamplePercent samples a percentage of documents.
	SamplePercent SampleMode = "percent"
)

// SamplePercentage creates SampleOptions for percentage-based sampling.
func SamplePercentage(percentage float64) *SampleOptions {
	return &SampleOptions{n: percentage, mode: SamplePercent}
}

// SampleDocLimit creates SampleOptions for document limit-based sampling.
func SampleDocLimit(documents int) *SampleOptions {
	return &SampleOptions{n: documents, mode: SampleDocuments}
}

func (s *SampleOptions) getProtoArgs() ([]*pb.Value, error) {
	nProto, err := encodeValue(s.n)
	if err != nil {
		return nil, err
	}
	modeProto := &pb.Value{ValueType: &pb.Value_StringValue{StringValue: string(s.mode)}}
	return []*pb.Value{nProto, modeProto}, nil
}

func newSampleStage(options *SampleOptions) *SampleStage {
	return &SampleStage{options: options}
}
func (s *SampleStage) name() string { return "sample" }
func (s *SampleStage) toStageProto() (*pb.Pipeline_Stage, error) {
	args, err := s.options.getProtoArgs()
	if err != nil {
		return nil, err
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: args,
	}, nil
}

// SortStage sorts documents based on specified orderings.
type SortStage struct {
	orders []*Ordering
}

func newSortStage(orders ...*Ordering) *SortStage {
	return &SortStage{orders: orders}
}
func (s *SortStage) name() string { return "sort" }
func (s *SortStage) toStageProto() (*pb.Pipeline_Stage, error) {
	protoOrders := make([]*pb.Value, len(s.orders))
	for i, o := range s.orders {
		pv, err := o.toProto()
		if err != nil {
			return nil, err
		}
		protoOrders[i] = pv
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: protoOrders,
	}, nil
}

// UnionStage performs a union operation with another pipeline.
type UnionStage struct {
	other *Pipeline
}

func newUnionStage(other *Pipeline) *UnionStage {
	return &UnionStage{other: other}
}
func (s *UnionStage) name() string { return "union" }
func (s *UnionStage) toStageProto() (*pb.Pipeline_Stage, error) {
	// The 'other' pipeline needs to be converted to its proto representation
	// which is a StructuredPipeline containing its stages.
	// This requires access to the internal toExecutePipelineRequest logic.
	// For simplicity, we'll assume a helper function or direct access to its proto stages.
	// In a real scenario, this might involve a recursive call or a specific proto conversion for Pipeline.
	otherReq, err := s.other.toExecutePipelineRequest()
	if err != nil {
		return nil, err
	}
	otherPipelineProto := otherReq.GetStructuredPipeline().GetPipeline()

	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: []*pb.Value{
			{ValueType: &pb.Value_PipelineValue{PipelineValue: otherPipelineProto}},
		},
	}, nil
}

// UnnestStage unnests an array field into separate documents.
type UnnestStage struct {
	field   *Field
	options *UnnestOptions
}

// UnnestOptions provides options for the Unnest stage.
type UnnestOptions struct {
	indexField string
}

// UnnestIndexField creates UnnestOptions with an index field.
func UnnestIndexField(indexField string) *UnnestOptions {
	return &UnnestOptions{indexField: indexField}
}

func newUnnestStage(field *Field, options *UnnestOptions) *UnnestStage {
	return &UnnestStage{field: field, options: options}
}
func (s *UnnestStage) name() string { return "unnest" }
func (s *UnnestStage) toStageProto() (*pb.Pipeline_Stage, error) {
	fieldProto, err := s.field.toProto()
	if err != nil {
		return nil, err
	}
	args := []*pb.Value{fieldProto}

	if s.options != nil && s.options.indexField != "" {
		optionsMap := map[string]*pb.Value{
			"index_field": {ValueType: &pb.Value_StringValue{StringValue: s.options.indexField}},
		}
		args = append(args, &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: optionsMap}}})
	}
	return &pb.Pipeline_Stage{
		Name: s.name(),
		Args: args,
	}, nil
}

// pipeline_utils.go

// encodeValue converts a Go any value to a Firestore protobuf Value.
// This is a simplified version; a real implementation would handle all Firestore types.
func encodeValue(value any) (*pb.Value, error) {
	switch v := value.(type) {
	case string:
		return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: v}}, nil
	case int:
		return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(v)}}, nil
	case int64:
		return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: v}}, nil
	case float64:
		return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: v}}, nil
	case bool:
		return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: v}}, nil
	case time.Time:
		return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(v)}}, nil
	case *DocumentRef:
		return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: v.Path}}, nil
	case []float64: // For vector types
		protoValues := make([]*pb.Value, len(v))
		for i, f := range v {
			protoValues[i] = &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}}
		}
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: protoValues}}}, nil
	case nil:
		return &pb.Value{ValueType: &pb.Value_NullValue{}}, nil
	case Expr:
		return v.toProto()
	case map[string]Expr: // For selectablesToMap helper
		fields := make(map[string]*pb.Value)
		for k, expr := range v {
			pv, err := expr.toProto()
			if err != nil {
				return nil, err
			}
			fields[k] = pv
		}
		return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: fields}}}, nil
	default:
		return nil, fmt.Errorf("firestore: unsupported value type for pipeline expression: %T", v)
	}
}

// toExprList converts a slice of any to a slice of Expr.
func toExprList(values ...any) []Expr {
	exprs := make([]Expr, len(values))
	for i, v := range values {
		if expr, ok := v.(Expr); ok {
			exprs[i] = expr
		} else {
			exprs[i] = ConstantOf(v)
		}
	}
	return exprs
}

// selectableMap is a helper struct to implement Selectable for a map of expressions.
type selectableMap struct {
	exprMap map[string]Expr
}

func newSelectableMap(fields map[string]Expr) *selectableMap {
	return &selectableMap{exprMap: fields}
}

func (s *selectableMap) isSelectable() {}
func (s *selectableMap) toProto() (*pb.Value, error) {
	protoFields := make(map[string]*pb.Value)
	for k, expr := range s.exprMap {
		pv, err := expr.toProto()
		if err != nil {
			return nil, err
		}
		protoFields[k] = pv
	}
	return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: protoFields}}}, nil
}

func selectablesToPbVal(selectables ...Selectable) (*pb.Value, error) {
	if len(selectables) == 0 {
		// TODO: THink more on this
		return nil, nil // An empty slice is valid, results in an empty map.
	}

	fields := make(map[string]bool, len(selectables))
	fieldsProto := make(map[string]*pb.Value, len(selectables))
	for _, s := range selectables {
		alias, protoVal, err := selectableToPbVal(s)
		if err != nil {
			return nil, err
		}
		if _, exists := fields[alias]; exists {
			return nil, fmt.Errorf("firestore: duplicate alias or field name %q in selectables", alias)
		}
		fields[alias] = true

		fieldsProto[alias] = protoVal
	}
	return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: fieldsProto}}}, nil
}

// selectablesToPbVals is a shared helper function that converts a slice of
// Selectable items into a map of alias->expression.
func selectablesToPbVals(selectables ...Selectable) ([]*pb.Value, error) {
	pbVal, err := selectablesToPbVal(selectables...)
	if err != nil {
		return nil, err
	}

	return []*pb.Value{pbVal}, nil
}

func selectableToPbVal(s Selectable) (string, *pb.Value, error) {
	alias, expr, err := s.getSelectionDetails()
	if err != nil {
		return alias, nil, err
	}
	protoVal, err := expr.toProto()
	if err != nil {
		return alias, nil, fmt.Errorf("error processing expression for alias %q in AddFields stage: %w", alias, err)
	}
	return alias, protoVal, nil
}
