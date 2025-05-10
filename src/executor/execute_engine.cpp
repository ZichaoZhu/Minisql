#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;

  pSyntaxNode column_list = ast->child_->next_;
  vector<string> column_names;
  vector<TypeId> column_types;
  vector<uint32_t> column_lengths;
  vector<uint32_t> column_index;
  uint32_t col_index = 0;
  vector<bool> column_is_unique;

  vector<string> primary_keys;
  // 遍历column子树
  for (auto col = column_list->child_; col != nullptr; col = col->next_) {
    if (col->type_ == kNodeColumnDefinition) {
      auto col_attributes = col->child_;
      string col_name;
      TypeId col_type;
      uint32_t col_length = 0;
      bool col_is_unique = false;

      col_name = col_attributes->val_;
      col_attributes = col_attributes->next_;
      col_is_unique = strcmp(col_attributes->val_, "unique") == 0;

      // 获取类型
      string type = col_attributes->val_;
      if (type == "int") {
        col_type = kTypeInt;
      }else if (type == "float") {
        col_type = kTypeFloat;
      }else if (type == "char") {
        col_type = kTypeChar;
        col_length = atoi(col_attributes->child_->val_);
        if (col_length <=0) {
          cout << "Invalid char length." << endl;
          return DB_FAILED;
        }
      }else {
        cout << "Invalid column type." << endl;
        return DB_FAILED;
      }

      column_names.push_back(col_name);
      column_types.push_back(col_type);
      column_lengths.push_back(col_length);
      column_index.push_back(col_index++);
      column_is_unique.push_back(col_is_unique);
    }
    else if (col->type_ == kNodeColumnList) {
      auto col_attributes = col->child_;
      while (col_attributes != nullptr && col_attributes->type_ == kNodeIdentifier) {
        string primary_key = col_attributes->val_;
        primary_keys.push_back(primary_key);
      }
    }
  }

  // 建立columns
  vector<Column *> columns;
  for (int index = 0; index < col_index; index++) {
    // primary key或者unique的时候，not nullable
    unordered_set<string> primary_keys_set = unordered_set<string>(primary_keys.begin(), primary_keys.end());
    bool is_primary_key = primary_keys_set.find(column_names[index]) != primary_keys_set.end();
    bool not_nullable = column_is_unique[index] || is_primary_key;
    if (column_types[index] == kTypeChar) {
      columns.push_back(new Column(column_names[index], column_types[index], column_lengths[index], index, not_nullable, column_is_unique[index]));
    }
    else {
      columns.push_back(new Column(column_names[index], column_types[index], index, not_nullable, column_is_unique[index]));
    }
  }

  // 创建表格
  Schema *schema = new Schema(columns);
  TableInfo *table_info;
  dberr_t result = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if (result != DB_SUCCESS) {
    return result;
  }
  // 创建索引
  if (!primary_keys.empty()) {
    IndexInfo *index_info;
    result = context->GetCatalog()->CreateIndex(table_info->GetTableName(), table_name + "_primary_key", primary_keys, context->GetTransaction(), index_info, "btree");
  }
  if (result != DB_SUCCESS) {
    return result;
    cout << "table " << table_info->GetTableName() << " created" << endl;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  // 首先，我们清理与表相关的索引
  string table_name = ast->child_->val_;
  // 先得到所有的索引信息
  vector<IndexInfo *> indexes;
  dberr_t result_get_index = context->GetCatalog()->GetTableIndexes(table_name, indexes);
  // 删除所有的索引
  for (auto index : indexes) {
    dberr_t result_drop_index = context->GetCatalog()->DropIndex(table_name, index->GetIndexName());
    if (result_drop_index != DB_SUCCESS) {
      cout << "Index " << index->GetIndexName() << " drop error" << endl;
      return result_drop_index;
    }
  }
  // 然后，我们删除表格
  dberr_t result_drop_table = context->GetCatalog()->DropTable(table_name);
  return result_drop_table;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  // 首先，我们找到数据库中的所有的表格
  vector<TableInfo *> tables_info;
  dberr_t result_get_table = context->GetCatalog()->GetTables(tables_info);
  if (result_get_table != DB_SUCCESS) {
    cout << "No table selected" << endl;
    return result_get_table;
  }

  // 定义字符串输出流
  stringstream ss;
  // 定义writer用于输出结果
  ResultWriter writer(ss);

  // 遍历table，输出index
  for (auto table = tables_info.begin(); table != tables_info.end(); table++) {
    vector<IndexInfo *> indexes_info;
    dberr_t result_get_indexes = context->GetCatalog()->GetTableIndexes((*table)->GetTableName(), indexes_info);
    // 检测是否查询到，查询到再进行接下来的操作
    if (result_get_indexes != DB_SUCCESS) {
      return result_get_indexes;
    }
    else {
      // 我们首先输出表格名字
      // 我们设计输出的表格是一列的，那么我们需要确定一个宽度
      vector<int> output_lengths = {static_cast<int>((*table)->GetTableName().length()) + 9};
      for (auto index : indexes_info) {
        if (index->GetIndexName().length() > output_lengths[0]) {
          output_lengths[0] = index->GetIndexName().length();
        }
      }
      // 我们输出表格的名字
      // 第一行为"Index in " + (*table)->GetTableName()，因此我们对output_length[0]进行一些调整
      output_lengths[0] += 9;  // len("Index in ") == 9
      writer.Divider(output_lengths);
      writer.BeginRow();
      writer.WriteHeaderCell("Index in " + (*table)->GetTableName(), output_lengths[0]);
      writer.EndRow();
      writer.Divider(output_lengths);

      // 输出索引
      for (auto index : indexes_info) {
        writer.BeginRow();
        writer.WriteCell(index->GetIndexName(), output_lengths[0]);
        writer.EndRow();
        writer.Divider(output_lengths);
      }
    }
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  // 提取语法树中的信息
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  vector<string> index_keys;
  pSyntaxNode indexes_list = ast->child_->next_->next_;
  string index_type = "btree";

  // 得到索引的列名
  for (auto index = indexes_list->child_; index != nullptr; index = index->next_) {
    index_keys.push_back(index->val_);
  }

  // 检查索引的类型
  if (indexes_list->next_ != nullptr) {
    index_type = indexes_list->next_->child_->val_;
  }

  //  创建索引
  IndexInfo *index_info;
  dberr_t result_create_index = context->GetCatalog()->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, index_type);

  if (result_create_index != DB_SUCCESS) {
    cout << "Create index error" << endl;
    return result_create_index;
  }

  TableInfo *table_info;
  dberr_t result_get_table = context->GetCatalog()->GetTable(table_name, table_info);
  if (result_get_table != DB_SUCCESS) {
    cout << "Get table error" << endl;
    return result_get_table;
  }

  // 将数据插入索引
  auto txn = context->GetTransaction();
  auto table_heap = table_info->GetTableHeap();
  for (auto row = table_heap->Begin(txn); row != table_heap->End(); row++) {
    auto row_id = row->GetRowId();
    // 获得相关的field
    vector<Field> fields;
    for (auto col : index_info->GetIndexKeySchema()->GetColumns()) {
      fields.push_back(*(*row).GetField(col->GetTableInd()));
    }
    // 将行插入索引
    Row row_idx(fields);
    dberr_t result_insert_entry = index_info->GetIndex()->InsertEntry(row_idx, row_id, txn);
    if (result_insert_entry != DB_SUCCESS) {
      return result_insert_entry;
    }
  }
  cout<<"index "<<index_name<<" created."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_FAILED;
}
