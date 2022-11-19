#include <memory>
#include <iostream>
#include <stdexcept>
#include <filesystem>

#include <arrow/array.h>
#include <arrow/builder.h>

#include <arrow/compute/exec.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>
#include <arrow/compute/exec/util.h>

#include <arrow/util/vector.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

namespace cp = ::arrow::compute;

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetSampleRecordBatch(
    const arrow::ArrayVector array_vector, const arrow::FieldVector& field_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto struct_result,
                        arrow::StructArray::Make(array_vector, field_vector));
  return record_batch->FromStructArray(struct_result);
}

arrow::Result<cp::ExecBatch> GetExecBatchFromVectors(
    const arrow::FieldVector& field_vector, const arrow::ArrayVector& array_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto res_batch, GetSampleRecordBatch(array_vector, field_vector));
  cp::ExecBatch batch{*res_batch};
  return batch;
}

struct BatchesWithSchema {
  std::vector<cp::ExecBatch> batches;
  std::shared_ptr<arrow::Schema> schema;
  // This method uses internal arrow utilities to
  // convert a vector of record batches to an AsyncGenerator of optional batches
  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> gen() const {
    auto opt_batches = ::arrow::internal::MapVector(
        [](cp::ExecBatch batch) { return std::make_optional(std::move(batch)); },
        batches);
    arrow::AsyncGenerator<std::optional<cp::ExecBatch>> gen;
    gen = arrow::MakeVectorGenerator(std::move(opt_batches));
    return gen;
  }
};

arrow::Result<BatchesWithSchema> MakeBasicBatches() {
  BatchesWithSchema out;
  auto field_vector = {arrow::field("a", arrow::int32()),
                       arrow::field("b", arrow::boolean())};
  ARROW_ASSIGN_OR_RAISE(auto b1_int, GetArrayDataSample<arrow::Int32Type>({0, 4}));
  ARROW_ASSIGN_OR_RAISE(auto b2_int, GetArrayDataSample<arrow::Int32Type>({5, 6, 7}));
  ARROW_ASSIGN_OR_RAISE(auto b3_int, GetArrayDataSample<arrow::Int32Type>({8, 9, 10}));

  ARROW_ASSIGN_OR_RAISE(auto b1_bool,
                        GetArrayDataSample<arrow::BooleanType>({false, true}));
  ARROW_ASSIGN_OR_RAISE(auto b2_bool,
                        GetArrayDataSample<arrow::BooleanType>({true, false, true}));
  ARROW_ASSIGN_OR_RAISE(auto b3_bool,
                        GetArrayDataSample<arrow::BooleanType>({false, true, false}));

  ARROW_ASSIGN_OR_RAISE(auto b1,
                        GetExecBatchFromVectors(field_vector, {b1_int, b1_bool}));
  ARROW_ASSIGN_OR_RAISE(auto b2,
                        GetExecBatchFromVectors(field_vector, {b2_int, b2_bool}));
  ARROW_ASSIGN_OR_RAISE(auto b3,
                        GetExecBatchFromVectors(field_vector, {b3_int, b3_bool}));

  out.batches = {b1, b2, b3};
  out.schema = arrow::schema(field_vector);
  return out;
}

arrow::Future<std::vector<arrow::compute::ExecBatch>> StartAndCollect(
    arrow::compute::ExecPlan* plan, arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>> gen) {
  RETURN_NOT_OK(plan->Validate());
  RETURN_NOT_OK(plan->StartProducing());

  auto collected_fut = CollectAsyncGenerator(gen);

  return arrow::AllFinished({plan->finished(), arrow::Future<>(collected_fut)})
      .Then([collected_fut]() -> arrow::Result<std::vector<arrow::compute::ExecBatch>> {
        ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
        return ::arrow::internal::MapVector(
            [](std::optional<arrow::compute::ExecBatch> batch) { return batch.value_or(arrow::compute::ExecBatch()); },
            std::move(collected));
      });
}

arrow::Status RunMain() {
    auto plan = cp::ExecPlan::Make();
    arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;

    ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

    cp::Declaration::Sequence(
    {
        {"source", cp::SourceNodeOptions{basic_data.schema,
                                     basic_data.gen()}},
        {"sink", cp::SinkNodeOptions{&sink_gen}},
    })
    .AddToPlan(plan->get());

    auto fut = StartAndCollect(plan->get(), sink_gen);
    fut.Wait();
    auto result = fut.result().ValueOrDie();
    auto actual = arrow::compute::TableFromExecBatches(basic_data.schema, result).ValueOrDie();

    // FIXME: Just print the table out for now
    std::cout << actual->ToString();

    // FIXME: This isn't right but it works. This should be some
    // Status-returning fn
    return arrow::Status();
}

int main()
{
    arrow::Status status = RunMain();

    if (status.ok()) {
        return EXIT_SUCCESS;
    } else {
        std::cout << "Error occurred: " << status.message() << std::endl;
        return EXIT_FAILURE;
    }
}
