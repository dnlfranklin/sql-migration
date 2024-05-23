<?php 

namespace SqlMigration\Provider\Mysql;

use SqlMigration\Catalog\Schema;
use SqlMigration\Catalog\Table;
use SqlMigration\Catalog\View;
use SqlMigration\Core\Connector;
use SqlMigration\Core\Differ;
use SqlMigration\Extension\MigratorExtension;

final class Migrator implements MigratorExtension{

    private Connector $connector;
    private Schema $schema_up;

    public function __construct(Connector $connector, Schema $schema_up){
        $this->connector = $connector;
        $this->schema_up = $schema_up;
    }

    public function map():Schema|null {
        $pdo = $this->connector->get();

        $stmt = $pdo->prepare('SELECT * FROM information_schema.schemata WHERE schema_name = ?');
        $stmt->bindValue(1, $this->schema_up->name);
        $stmt->execute();
        $data = $stmt->fetch(); 

        if(!$data){
            return null;
        }

        $schema = new Schema($data['SCHEMA_NAME'], $data['DEFAULT_CHARACTER_SET_NAME'], $data['DEFAULT_COLLATION_NAME']);
        
        $stmt = $pdo->prepare('
            SELECT 
                * 
            FROM 
                information_schema.tables t 
            LEFT JOIN 
                information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa 
                    ON CCSA.collation_name = t.table_collation 
            WHERE 
                table_schema = ?');
        $stmt->bindValue(1, $schema->name);  
        $stmt->execute();
        $data_tables = $stmt->fetchAll();

        foreach($data_tables as $data_table){
            switch($data_table['TABLE_TYPE']){
                case 'BASE TABLE':
                    $table = new Table(
                        $data_table['TABLE_NAME'], 
                        $data_table['ENGINE'], 
                        $data_table['CHARACTER_SET_NAME'], 
                        $data_table['TABLE_COLLATION'], 
                        $data_table['TABLE_COMMENT']
                    );
                    
                    $stmt = $pdo->prepare('
                        SELECT 
                            table_schema AS table_schema,
                            table_name AS table_name,
                            ordinal_position AS ordinal_position,
                            column_name AS column_name,
                            data_type AS data_type,
                            column_type AS column_type,
                            case 
                                when numeric_precision is not null then numeric_precision 
                                else character_maximum_length 
                            end as max_length,
                            case 
                                when datetime_precision is not null then datetime_precision 
                                when numeric_scale is not null then numeric_scale
                                else null
                            end as data_precision,
                            is_nullable AS is_nullable,
                            column_default AS column_default,
                            extra AS extra
                        FROM 
                            information_schema.columns
                        WHERE 
                            table_schema = ? and table_name = ?
                        ORDER BY 
                            ordinal_position
                    ');
                    $stmt->bindValue(1, $schema->name);
                    $stmt->bindValue(2, $table->name);
                    $stmt->execute();

                    foreach($stmt->fetchAll() as $column){
                        $column_type = explode(' ', $column['column_type'])[0];
                        $column_type = str_replace([$column['data_type'], '(', ')'], '', $column_type);

                        if($column_type == ''){
                            $column_length = null;
                            $column_precision = null;
                        }else{
                            $length_precision = explode(',', $column_type);
                            $column_length = empty($length_precision[0]) ? null : (int) $length_precision[0];
                            $column_precision = empty($length_precision[1]) ? null : (int) $length_precision[1];
                        }
                        
                        $table->addColumn(
                            $column['column_name'],
                            $column['data_type'],
                            $column_length,
                            $column_precision,
                            $column['column_default'],
                            $column['is_nullable'] == 'YES' ? true : false,
                            stristr($column['extra'], 'auto_increment') !== FALSE ? true : false,
                            stristr($column['column_type'], 'zerofill') !== FALSE ? true : false,
                            stristr($column['column_type'], 'unsigned') !== FALSE ? true : false
                        );
                    }

                    $stmt = $pdo->prepare("
                        SELECT 
                            stat.table_schema AS table_schema,
                            stat.table_name AS table_name,
                            stat.index_name AS index_name,
                            group_concat(stat.column_name order by stat.seq_in_index separator ',') as columns,
                            case 
                                when tco.constraint_type IS NULL then 'KEY' 
                                else tco.constraint_type 
                            end as constraint_type
                        FROM 
                            information_schema.statistics stat
                        LEFT JOIN 
                            information_schema.table_constraints tco on 
                                tco.table_name = stat.table_name and 
                                tco.table_schema = stat.table_schema and 
                                tco.constraint_name = stat.index_name and 
                                tco.constraint_type != 'FOREIGN KEY'
                        WHERE 
                            stat.table_schema = ? and stat.table_name = ?
                        GROUP BY
                            stat.table_schema,
                            stat.table_name,
                            stat.index_name,
                            tco.constraint_type
                    ");
                    $stmt->bindValue(1, $schema->name);
                    $stmt->bindValue(2, $table->name);
                    $stmt->execute();

                    foreach($stmt->fetchAll() as $key){
                        $table->addKey($key['index_name'], $key['columns'], $key['constraint_type']);
                    }

                    $stmt = $pdo->prepare("
                        SELECT   
                                fks.constraint_schema as foreign_schema,
                                fks.table_name as foreign_table,
                                kcu.column_name as foreign_column_name,
                                fks.constraint_name as foreign_constraint_name,
                                fks.unique_constraint_schema as reference_schema,
                                fks.referenced_table_name as reference_table,
                                kcu.referenced_column_name as reference_column_name
                        FROM information_schema.referential_constraints fks
                        JOIN information_schema.key_column_usage kcu
                            on fks.constraint_schema = kcu.table_schema
                            and fks.table_name = kcu.table_name
                            and fks.constraint_name = kcu.constraint_name
                            and kcu.POSITION_IN_UNIQUE_CONSTRAINT IS NOT NULL
                        WHERE 
                            fks.constraint_schema = ? AND fks.table_name = ?
                        ORDER BY 
                            kcu.ordinal_position
                    ");
                    $stmt->bindValue(1, $schema->name);
                    $stmt->bindValue(2, $table->name);
                    $stmt->execute();

                    foreach($stmt->fetchAll() as $fk){
                        $table->addForeignKey(
                            $fk['foreign_constraint_name'], 
                            $fk['foreign_column_name'], 
                            $fk['reference_schema'],
                            $fk['reference_table'],
                            $fk['reference_column_name'],
                        );
                    }

                    $schema->addTable($table);
                    break;
                case 'VIEW':
                    $stmt = $pdo->prepare('SELECT * FROM information_schema.views WHERE table_schema = ? and table_name = ?');
                    $stmt->bindValue(1, $schema->name);
                    $stmt->bindValue(2, $data_table['TABLE_NAME']);
                    $stmt->execute();

                    $data_view = $stmt->fetch();

                    $view = new View($data_view['TABLE_NAME'], $data_view['VIEW_DEFINITION']);

                    $stmt = $pdo->prepare('
                        SELECT 
                                col.table_schema as database_name,
                                col.table_name as view_name,
                                col.ordinal_position as ordinal_position,
                                col.column_name as column_name
                        FROM 
                            information_schema.columns col
                        JOIN 
                            information_schema.views vie on vie.table_schema = col.table_schema
                                                            and vie.table_name = col.table_name
                        WHERE 
                            col.table_schema = ? and col.table_name = ?
                        ORDER BY 
                            col.ordinal_position
                    ');
                    $stmt->bindValue(1, $schema->name);
                    $stmt->bindValue(2, $view->name);
                    $stmt->execute();

                    foreach($stmt->fetchAll() as $view_column){
                        $view->addColumn($view_column['column_name']);
                    }

                    $schema->addView($view);

                    break;
            }            
        }

        return $schema;
    }

    public function sqlInstructions(bool $migrate_data = false):Array {
        $schema_base = $this->map();
        
        $diff = (new Differ($this->schema_up, $schema_base))->analyze();
        
        $sql = [
            'SCHEMA' => [],
            'DROP_FK' => [],
            'DROP_KEY' => [],
            'DROP_COLUMN' => [],
            'DROP_TABLE' => [],
            'TABLE' => [],
            'COLUMN' => [],
            'KEY' => [],
            'FOREIGN KEY' => [],
            'VIEW' => []
        ];

        foreach($diff as $step){
            if(!$step['action']){
                continue;
            }

            switch($step['catalog']){
                case 'SCHEMA':
                    $sql['SCHEMA'][] = "CREATE SCHEMA `{$this->schema_up->name}`;";
                    break;
                case 'SCHEMA CHARSET':
                    $sql['SCHEMA'][] = "ALTER SCHEMA `{$this->schema_up->name}`  DEFAULT CHARACTER SET {$this->schema_up->charset};";
                    break;
                case 'SCHEMA COLLATION':
                    $sql['SCHEMA'][] = "ALTER SCHEMA `{$this->schema_up->name}`  DEFAULT COLLATE {$this->schema_up->collation};";
                    break;
                case 'DROP TABLE':
                    $drop_table_name = $step['identifier'];

                    $sql['DROP_TABLE'][] = "DROP TABLE `{$this->schema_up->name}`.`{$drop_table_name}`";    
                    break;
                case 'DROP COLUMN':
                    $drop_table_name = $step['parent'];
                    $drop_name = $step['identifier'];

                    $sql['DROP_COLUMN'][] = "ALTER TABLE `{$this->schema_up->name}`.`{$drop_table_name}` DROP COLUMN `{$drop_name}`;"; 
                    break;
                case 'DROP KEY':
                    $drop_table_name = $step['parent'];
                    $drop_name = $step['identifier'];

                    $sql['DROP_KEY'][] = "ALTER TABLE `{$this->schema_up->name}`.`{$drop_table_name}` DROP INDEX `{$drop_name}`;";
                    break;
                case 'DROP FOREIGN KEY':
                        $drop_table_name = $step['parent'];
                        $drop_name = $step['identifier'];
    
                        $sql['DROP_FK'][] = "ALTER TABLE `{$this->schema_up->name}`.`{$drop_table_name}` DROP FOREIGN KEY `{$drop_name}`;";
                    break;
                case 'TABLE':
                    $table = $this->schema_up->tables[$step['identifier']];

                    if($step['action'] == 'UPDATE'){
                        $sql['TABLE'][] = "ALTER TABLE `{$this->schema_up->name}`.`{$table->name}` CHARACTER SET = {$table->charset} , COLLATE = {$table->collation} , ENGINE = {$table->engine} , COMMENT = '{$table->comments}';";
                    }
                    else{
                        $create_table = "CREATE TABLE `{$this->schema_up->name}`.`{$table->name}` ({COLUMNS_KEYS}) ENGINE = {$table->engine} DEFAULT CHARACTER SET = {$table->charset} COLLATE = {$table->collation} COMMENT = '{$table->comments}';";

                        $column_keys = [];

                        foreach($table->columns as $column){
                            $column_sql = "`{$column->name}` {$column->type}";

                            if(is_null($column->length) && is_null($column->precision)){
                                $column_sql.= '';    
                            }
                            else if(!is_null($column->length) && !is_null(!$column->precision) && $column->precision != 0){
                                $column_sql.= "({$column->length},{$column->precision})";
                            }
                            else{
                                $column_sql.= is_null($column->length) ? "({$column->precision})" : "({$column->length})";
                            }

                            $column_sql.= $column->unsigned ? ' unsigned' : '';
                            $column_sql.= $column->zerofill ? ' zerofill' : '';
                            $column_sql.= $column->nullable ? '' : ' NOT NULL';
                            $column_sql.= $column->auto_increment ? ' AUTO_INCREMENT' : '';
                            $column_sql.= is_null($column->default) ? '': " DEFAULT '{$column->default}'";

                            $column_keys[] = $column_sql;
                        }

                        foreach($table->keys as $key){
                            $key_columns = [];
                            foreach(explode(',', $key->columns) as $key_column){
                                $key_columns[] = "`{$key_column}`";
                            }
                            $key_columns = implode(',', $key_columns);
                            
                            switch($key->type){
                                case 'PRIMARY KEY':
                                    $column_keys[] = "PRIMARY KEY ({$key_columns})";                                
                                    break;
                                case 'UNIQUE':
                                    $column_keys[] = "UNIQUE KEY `{$key->name}` ({$key_columns})";                                
                                    break;
                                case 'KEY':
                                    $column_keys[] = "KEY `{$key->name}` ({$key_columns})";
                                    break;
                            }                            
                        }
                        
                        $sql['TABLE'][] = str_replace('{COLUMNS_KEYS}', implode(',', $column_keys), $create_table);
                    }
                    break;
                    case 'COLUMN':
                        $table_name = $step['parent']; 
                        $column_name = $step['identifier'];
                        
                        $column = $this->schema_up->tables[$table_name]->columns[$column_name];

                        $cmd = $step['action'] == 'UPDATE' ? "CHANGE COLUMN `{$column->name}`" : "ADD COLUMN";

                        $column_sql = "ALTER TABLE `{$this->schema_up->name}`.`$table_name` {$cmd} `{$column->name}` {$column->type}";

                        if(is_null($column->length) && is_null($column->precision)){
                            $column_sql.= '';    
                        }
                        else if(!is_null($column->length) && !is_null(!$column->precision) && $column->precision != 0){
                            $column_sql.= "({$column->length},{$column->precision})";
                        }
                        else{
                            $column_sql.= is_null($column->length) ? "({$column->precision})" : "({$column->length})";
                        }

                        $column_sql.= $column->unsigned ? ' unsigned' : '';
                        $column_sql.= $column->zerofill ? ' zerofill' : '';
                        $column_sql.= $column->nullable ? ' NULL' : ' NOT NULL';
                        $column_sql.= $column->auto_increment ? ' AUTO_INCREMENT' : '';
                        $column_sql.= is_null($column->default) ? ';': " DEFAULT '{$column->default}';";

                        $sql['COLUMN'][] = $column_sql;
                        break;
                    case 'KEY':
                        $table_name = $step['parent'];
                        $key_name = $step['identifier'];
                        
                        $key = $this->schema_up->tables[$table_name]->keys[$key_name];

                        $key_columns = [];
                        foreach(explode(',', $key->columns) as $key_column){
                            $key_columns[] = "`{$key_column}`";
                        }
                        $key_columns = implode(',', $key_columns);
                        
                        $key_sql = "ALTER TABLE `{$this->schema_up->name}`.`$table_name` ADD ";

                        switch($key->type){
                            case 'PRIMARY KEY':
                                $key_sql.= "PRIMARY KEY ({$key_columns});";                                
                                break;
                            case 'UNIQUE':
                                $key_sql.= "UNIQUE KEY `{$key->name}` ({$key_columns});";                                
                                break;
                            case 'KEY':
                                $key_sql.= "KEY `{$key->name}` ({$key_columns});";
                                break;
                        }

                        if($step['action'] == 'UPDATE'){
                            $sql['KEY'][] = "ALTER TABLE `{$this->schema_up->name}`.`$table_name` DROP INDEX `{$key->name}`;";
                        }

                        $sql['KEY'][] = $key_sql;
                        break;
                    case 'FOREIGN KEY':
                        $table_name = $step['parent'];
                        $fk_name = $step['identifier'];
                        
                        $fk = $this->schema_up->tables[$table_name]->fks[$fk_name];

                        if($step['action'] == 'UPDATE'){
                            $sql['FOREIGN KEY'][] = "ALTER TABLE `{$this->schema_up->name}`.`$table_name` DROP FOREIGN KEY `{$fk->name}`;";
                        }                            

                        $sql['FOREIGN KEY'][] = "ALTER TABLE `{$this->schema_up->name}`.`$table_name` ADD CONSTRAINT `{$fk->name}` FOREIGN KEY (`{$fk->fk_column}`) REFERENCES `{$fk->reference_schema}`.`{$fk->reference_table}` (`{$fk->reference_column}`);";
                        break;
                    case 'VIEW':
                        $view = $this->schema_up->views[$step['identifier']];

                        $sql['VIEW'][] = "CREATE OR REPLACE VIEW `{$this->schema_up->name}`.`{$view->name}` AS $view->def;";
                        break;
            }
        }

        if($migrate_data){
            foreach($this->schema_up->data as $data){
                $rows = [];

                foreach($data->columns_data as $column_row){
                    $row = [];

                    foreach($column_row as $column){
                        if(is_string($column)){
                            $row[] = "'{$column}'";
                        }
                        else if(is_null($column)){
                            $row[] = "NULL";
                        }
                        else{
                            $row[] = $column;
                        }
                    }

                    if(!empty($row)){
                        $row =  implode(',', $row);

                        $rows[] = "({$row})";
                    }
                }
                
                if(!empty($rows)){
                    $columns_name = implode(',', $data->columns_name); 
                    $columns_rows = implode(',', $rows); 

                    $sql['DATA'][] = "INSERT INTO `{$this->schema_up->name}`.`$data->table_name` ($columns_name) VALUES {$columns_rows};";                
                }                
            }
        }
        
        return $sql;
    }

    public function migrate(bool $migrate_data = false):Array {
        $sql_log = [];
        
        foreach($this->sqlInstructions($migrate_data) as $group => $commands){
            foreach($commands as $cmd){
                $sql_log[] = [
                    'cmd' => $cmd,
                    'status' => 'NOT EXECUTED',
                    'group' => $group
                ];
            }
        }       

        $pdo = $this->connector->get();
        
        try{
            foreach($sql_log as $key => $item){
                $cmd = $item['cmd'];
                $group = $item['group'];
            
                $pdo->exec($cmd);

                $sql_log[$key]['status'] = 'EXECUTED';                     
            }
        }catch(\Throwable $e){
            $sql_log[$key]['status'] = 'ERROR';                     
            $sql_log[$key]['details'] = $e->getMessage();                     
        }finally{
            return $sql_log;
        }
    }

}