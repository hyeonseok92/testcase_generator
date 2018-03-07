#include <iostream>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <unordered_set>
#include <cstdint>
#include <functional>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <unistd.h>
#define IGN_RET(x) if (x);

using namespace std;
using RelationId=unsigned;

string kTestcase;
string kDirPath;
uint64_t kNumTable;

uint64_t kMaxNumRow;
uint64_t kMinNumRow;
uint64_t kMaxNumCol;
uint64_t kMinNumCol;

uint64_t kKeyRange;
uint64_t kNumQuery;
uint64_t kBatchSize;

uint64_t kMinNumPredicate;
uint64_t kMaxNumPredicate;
uint64_t kMinNumFilter;
uint64_t kMaxNumFilter;
uint64_t kMinNumSelect;
uint64_t kMaxNumSelect;

uint64_t randomRange(uint64_t min, uint64_t max)
{
	return rand() % (max + 1 - min) + min;
}

class Relation{
public:
	uint64_t size;
	vector< unique_ptr<uint64_t[]> > columns;

	Relation(uint64_t size, vector< unique_ptr<uint64_t[]> > &&columns) : size(size){
		this->columns.clear();
		for (auto &c : columns){
			this->columns.emplace_back(move(c));
		}
	}

	void storeRelation(const string &file_name)
	{
		ofstream file;
		file.open(kDirPath + file_name,ios::out|ios::binary);
		file.write(reinterpret_cast<char*>(&size),sizeof(size));
		size_t tmp = columns.size();
		file.write(reinterpret_cast<char*>(&tmp),sizeof(tmp));
		for (auto &c : columns) {
			file.write(reinterpret_cast<char*>(c.get()), size * sizeof(uint64_t));
		}
		file.close();
	}

	void storeRelationCSV(const string &file_name)
	{
		ofstream file;
		file.open(kDirPath + file_name+".tbl",ios::out);
		for (uint64_t i = 0; i < size; ++i) {
			for (auto& c : columns) {
				file << c[i] << '|';
			}
			file << "\n";
		}
		file.close();
	}
};

vector<Relation> tables;

bool applyConfig(const string &key, const string &value)
{
#define CONFIG_SET_VARIABLE_STRING(x, ptr) do{	\
	if(key.compare(x) == 0){	\
		*(ptr) = value;	\
		return true;	\
	}	\
}while(0)

#define CONFIG_SET_VARIABLE_ULONGLONG(x, ptr) do{	\
	if(key.compare(x) == 0){	\
		*(ptr) = stoull(value);	\
		return true;	\
	}	\
}while(0)

	CONFIG_SET_VARIABLE_ULONGLONG("num_table", &kNumTable);

	CONFIG_SET_VARIABLE_ULONGLONG("min_num_row", &kMinNumRow);
	CONFIG_SET_VARIABLE_ULONGLONG("max_num_row", &kMaxNumRow);
	CONFIG_SET_VARIABLE_ULONGLONG("min_num_col", &kMinNumCol);
	CONFIG_SET_VARIABLE_ULONGLONG("max_num_col", &kMaxNumCol);

	CONFIG_SET_VARIABLE_ULONGLONG("key_range", &kKeyRange);
	CONFIG_SET_VARIABLE_ULONGLONG("num_query", &kNumQuery);
	CONFIG_SET_VARIABLE_ULONGLONG("batch_size", &kBatchSize);

	CONFIG_SET_VARIABLE_ULONGLONG("min_predicate", &kMinNumPredicate);
	CONFIG_SET_VARIABLE_ULONGLONG("max_predicate", &kMaxNumPredicate);
	CONFIG_SET_VARIABLE_ULONGLONG("min_filter", &kMinNumFilter);
	CONFIG_SET_VARIABLE_ULONGLONG("max_filter", &kMaxNumFilter);
	CONFIG_SET_VARIABLE_ULONGLONG("min_select", &kMinNumSelect);
	CONFIG_SET_VARIABLE_ULONGLONG("max_select", &kMaxNumSelect);
	return false;
}

void makeDir(){
	kDirPath = kTestcase + "/";
	string rmCmd = string("rm -rf ") + kDirPath;
	IGN_RET(system(rmCmd.c_str()));
	string mkdirCmd = string("mkdir -p ") + kDirPath;
	IGN_RET(system(mkdirCmd.c_str()));
}

void parseConfig(const string &str)
{
	string filepath = str + string(".conf");
	ifstream file(filepath);
	string line;
	while(getline(file, line)){
		if (line.size() == 0 || line[0] == '#' || line[0] == '[') continue;
		cout << line << '\n';
		line.erase(remove(line.begin(), line.end(), ' '), line.end());
		string attr_key = line.substr(0, line.find('='));
		string attr_value = line.substr(attr_key.size()+1);
		if (!applyConfig(attr_key, attr_value)){
			cerr << "Error at parsing\n";
			throw;
		}
	}
	file.close();
}

void makeTable()
{
	for (int i = 0; i < kNumTable; ++i){
		vector< unique_ptr<uint64_t[]> > table;
		string table_name = string("r") + to_string(i);
		uint64_t num_row = randomRange(kMinNumRow, kMaxNumRow);
		uint64_t num_col = randomRange(kMinNumCol, kMaxNumCol);
		for (int j = 0; j < num_col; ++j){
			unique_ptr<uint64_t[]> ptr = make_unique<uint64_t[]>(num_row);
			for (int k = 0; k < num_row; ++k){
				ptr[k] = randomRange(1, kKeyRange);
			}
			table.emplace_back(move(ptr));
		}

		Relation r(num_row, move(table));
		r.storeRelation(table_name);
		r.storeRelationCSV(table_name);
		tables.emplace_back(move(r));
	}
}

void makeInit()
{
	makeTable();
	ofstream file(kDirPath + kTestcase + string(".init"));
	ofstream all_file(kDirPath + kTestcase + string(".all"));
	for (int i = 0; i < kNumTable; ++i){
		file << "r" << i << '\n';
		all_file << "r" << i << '\n';
	}
	all_file << "Done" << '\n';
	file.close();
	all_file.close();
}

vector<RelationId> makeRelations()
{
	uint64_t num_relation = randomRange(2, 4);
	vector<RelationId> relation_ids;
	for (RelationId i = 0; i < kNumTable; ++i){
		relation_ids.emplace_back(i);
	}
	for (uint64_t i = 0; i < kNumTable - num_relation; ++i){
		relation_ids.erase(relation_ids.begin() + randomRange(0, kNumTable - i - 1));
	}

	return move(relation_ids);
}

string makePredicates(const vector<RelationId> &relation_ids)
{
	if (relation_ids.size() == 1){
		return string("");
	}
	while(1){
		uint64_t num_predicate = randomRange(kMinNumPredicate, kMaxNumPredicate);
		ostringstream oss;
		int group[4] = {0,}, group_gen = 0;
		for (uint64_t i = 0; i < num_predicate; ++i){
			vector<RelationId> pred_rels(relation_ids);
			vector<unsigned> bindings;
			for (uint64_t j = 0; j < relation_ids.size(); ++j){
				bindings.emplace_back(j);
			}
			for (uint64_t j = 0; j < relation_ids.size() - 2; ++j){
				unsigned rm_idx = static_cast<unsigned>(randomRange(0, relation_ids.size() - j - 1));
				pred_rels.erase(pred_rels.begin() + rm_idx);
				bindings.erase(bindings.begin() + rm_idx);
			}
			if (group[bindings[0]] == 0 && group[bindings[1]] == 0){
				group[bindings[0]] = group[bindings[1]] = ++group_gen;
			}
			else if (group[bindings[0]] == 0){
				group[bindings[0]] = group[bindings[1]];
			}
			else if (group[bindings[1]] == 0){
				group[bindings[1]] = group[bindings[0]];
			}
			else {
				int change_group = group[bindings[1]];
				for (int k = 0; k < 4; ++k){
					if (group[k] == change_group){
						group[k] = group[bindings[0]];
					}
				}
			}
			oss << bindings[0] << "." << randomRange(0, tables[pred_rels[0]].columns.size() - 1);
			oss << "=";
			oss << bindings[1] << "." << randomRange(0, tables[pred_rels[1]].columns.size() - 1);
			if (i != num_predicate - 1){
				oss << "&";
			}
		}
		//Connect Check
		
		int first_group = group[0];
		bool not_connected = false;
		for (int i = 0; i < relation_ids.size(); ++i){
			if (group[i] != first_group){
				not_connected = true;
				break;
			}
		}
		if (not_connected)
			continue;
		return oss.str();
	}
}

string makeFilters(const vector<RelationId> &relation_ids)
{
	uint64_t num_filter = randomRange(kMinNumFilter, kMaxNumFilter);
	char comparer[3] = {'>', '=', '<'};
	ostringstream oss;
	for (uint64_t i = 0; i < num_filter ; ++i){
		unsigned bind = static_cast<unsigned>(randomRange(0, relation_ids.size() - 1));
		oss << bind << "." << randomRange(0, tables[relation_ids[bind]].columns.size() - 1);
		oss << comparer[randomRange(0, 2)] << randomRange(0, kKeyRange-1);
		if (i != num_filter-1){
			oss << '&';
		}
	}
	return oss.str();
}

string makeSelects(const vector<RelationId> &relation_ids){
	uint64_t num_select = randomRange(kMinNumSelect, kMaxNumSelect);
	ostringstream oss;
	unordered_set<string> dup_check;
	for (uint64_t i = 0; i < num_select; ++i){
		unsigned bind = static_cast<unsigned>(randomRange(0, relation_ids.size() - 1));
		string selected =
			to_string(bind) + "." +
			to_string(randomRange(0, tables[relation_ids[bind]].columns.size() - 1));
		if (dup_check.find(selected) != dup_check.end()){
			--i;
			continue;
		}

		oss << selected;
		if (i != num_select - 1){
			oss << " ";
		}
	}
	return oss.str();
}

string makeQuery()
{
	ostringstream oss;
	uint64_t num_selection = randomRange(kMinNumSelect, kMaxNumSelect);
	vector<RelationId> relation_ids = makeRelations();
	for (uint64_t i = 0; i < relation_ids.size(); i++){
		oss << relation_ids[i];
		if (i != relation_ids.size() - 1){
			oss << ' ';
		}
	}
	oss << '|';
	string predicates = makePredicates(relation_ids);
	oss << predicates;
	if (!predicates.empty()){
		oss << '&';
	}
	oss << makeFilters(relation_ids);
	oss << '|';
	oss << makeSelects(relation_ids);
	return oss.str();
}

void makeWorkload()
{
	ofstream file(kDirPath + kTestcase + string(".work"));
	ofstream all_file(kDirPath + kTestcase + string(".all"), ofstream::app);
	for (int i = 0; i < kNumQuery; i++){
		string query = makeQuery(); 
		file << query << '\n';
		all_file << query << '\n';
		if ((i+1) % kBatchSize == 0){
			file << "F" << '\n';
			all_file << "F" << '\n';
		}
	}
	if (kNumQuery % kBatchSize != 0){
		file << "F";
		all_file << "F";
	}
	file.close();
	all_file.close();
}

void makeResult(){
	kDirPath = kTestcase + "/";
	string input = kTestcase + string(".all");
	string output = kTestcase + string(".result");
	char cur_dir[4096];
	IGN_RET(getcwd(cur_dir, sizeof(cur_dir)));
	IGN_RET(chdir(kDirPath.c_str()));
	string cmd = string("../Driver < ") + input + " > " + output;
	IGN_RET(system(cmd.c_str()));
	IGN_RET(chdir(cur_dir));
}

int main(int argc, char *argv[])
{
	if (argc != 2){
		printf("Usage : %s [testcase]\n", argv[0]);
		printf("Example : %s small\n", argv[0]);
		return 0;
	}
	srand(time(NULL));
	kTestcase = string(argv[1]);
	kTestcase = kTestcase.substr(0, kTestcase.find_last_of("."));
	makeDir();
	parseConfig(kTestcase);
	makeInit();
	makeWorkload();
	makeResult();
	return 0;
}
