<?php

namespace SqlMigration\Core;

use SqlMigration\Catalog\Column;
use SqlMigration\Catalog\ForeignKey;
use SqlMigration\Catalog\Key;
use SqlMigration\Catalog\Schema;
use SqlMigration\Catalog\Table;

class Instruction{

    private $groups = [
        'SCHEMA'      => [],
        'DROP_VIEW'   => [],
        'DROP_FK'     => [],
        'DROP_KEY'    => [],
        'DROP_COLUMN' => [],
        'DROP_TABLE'  => [],
        'TABLE'       => [],
        'COLUMN'      => [],
        'KEY'         => [],
        'FOREIGN_KEY' => [],
        'VIEW'        => [],
        'DATA'        => [],
        'UNKNOWN'     => []
    ];

    public function __construct(private Schema $schema){}

    public function getSql():Array {
        $sql = [];
        
        foreach($this->groups as $group){
            foreach($group as $cmd){
                $sql[] = $cmd;
            }
        }

        return $sql;
    }

    public function getExecutables():Array {
        $execs = [];
        
        foreach($this->groups as $group => $sqls){
            foreach($sqls as $sql){
                $execs[] = new \SqlMigration\Core\Executable($sql, $group);
            }
        }

        return $execs;
    }

    public function add(string $custom_sql, string $level = null){
        if(!$level || !isset($this->groups[$level])){
            $level = 'UNKNOWN';
        }
        
        $this->groups[$level][] = $custom_sql;
    }

    public function getTable(string $table_name):Table{
        $table = $this->schema->tables[$table_name] ?? null;

        if(!$table){
            throw new \Exception("Table {$table_name} does not exists in the Schema");
        }
        
        return $table;
    }

    public function getColumn(string $table_name, string $column_name):Column{
        $column = $this->getTable($table_name)->columns[$column_name] ?? null;

        if(!$column){
            throw new \Exception("Column {$table_name}.{$column_name} does not exists in the Schema");
        }
        
        return $column;
    }

    public function getKey(string $table_name, string $key_name):Key{
        $key = $this->getTable($table_name)->keys[$key_name] ?? null;

        if(!$key){
            throw new \Exception("Key {$table_name}.{$key_name} does not exists in the Schema");
        }
        
        return $key;
    }

    public function getForeignKey(string $table_name, string $fk_name):ForeignKey{
        $fk = $this->getTable($table_name)->fks[$fk_name] ?? null;

        if(!$fk){
            throw new \Exception("Foreign Key {$table_name}.{$fk_name} does not exists in the Schema");
        }
        
        return $fk;
    }    
    
    public function createSchema(){
        $sql_create = "CREATE SCHEMA `{$this->schema->name}`";

        if(!empty($this->schema->charset)){
            $sql_create.= " DEFAULT CHARACTER SET {$this->schema->charset}";
        }       

        if(!empty($this->schema->collation)){
            $sql_create.= " DEFAULT COLLATE {$this->schema->collation}";
        }
        
        $this->add("{$sql_create};", 'SCHEMA');
    }

    public function alterSchemaCharset(){
        $this->add("ALTER SCHEMA `{$this->schema->name}`  DEFAULT CHARACTER SET {$this->schema->charset};", 'SCHEMA');    
    }

    public function alterSchemaCollation(){
        $this->add("ALTER SCHEMA `{$this->schema->name}`  DEFAULT COLLATE {$this->schema->collation};", 'SCHEMA');    
    }    

    public function createTable(string $table_name){
        $table = $this->getTable($table_name);
        
        $create_table = "CREATE TABLE `{$this->schema->name}`.`{$table->name}` ({COLUMNS_KEYS}) ENGINE = {$table->engine} DEFAULT CHARACTER SET = {$table->charset} COLLATE = {$table->collation} COMMENT = '{$table->comments}';";

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
            $column_sql.= is_null($column->default) ? '': " DEFAULT ".self::formatConstantValue($column->default);

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
        
        $this->add(str_replace('{COLUMNS_KEYS}', implode(',', $column_keys), $create_table), 'TABLE');
    }

    public function alterTable(string $table_name){
        $table = $this->getTable($table_name);
        
        $this->add("ALTER TABLE `{$this->schema->name}`.`{$table->name}` CHARACTER SET = {$table->charset} , COLLATE = {$table->collation} , ENGINE = {$table->engine} , COMMENT = '{$table->comments}';", 'TABLE');
    }

    public function changeColumn(string $table_name, string $column_name, bool $update = false){
        $column = $this->getColumn($table_name, $column_name);

        $cmd = $update ? "CHANGE COLUMN `{$column->name}`" : "ADD COLUMN";

        $column_sql = "ALTER TABLE `{$this->schema->name}`.`$table_name` {$cmd} `{$column->name}` {$column->type}";

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
        $column_sql.= is_null($column->default) ? '': " DEFAULT ".self::formatConstantValue($column->default);
        $column_sql.= ';';

        $this->add($column_sql, 'COLUMN');
    }

    public function changeKey(string $table_name, string $key_name, bool $update = false){
        $key = $this->getKey($table_name, $key_name);

        $key_columns = [];
        foreach(explode(',', $key->columns) as $key_column){
            $key_columns[] = "`{$key_column}`";
        }
        $key_columns = implode(',', $key_columns);
        
        $key_sql = "ALTER TABLE `{$this->schema->name}`.`$table_name` ADD ";

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

        if($update){
            $this->add("ALTER TABLE `{$this->schema->name}`.`$table_name` DROP INDEX `{$key->name}`;", 'KEY');
        }

        $this->add($key_sql, 'KEY');
    }

    public function changeForeignKey(string $table_name, string $fk_name, bool $update = false){
        $fk = $this->getForeignKey($table_name, $fk_name);

        if($update){
            $this->add("ALTER TABLE `{$this->schema->name}`.`$table_name` DROP FOREIGN KEY `{$fk->name}`;", 'FOREIGN_KEY');
        }                            

        $this->add("ALTER TABLE `{$this->schema->name}`.`$table_name` ADD CONSTRAINT `{$fk->name}` FOREIGN KEY (`{$fk->fk_column}`) REFERENCES `{$fk->reference_schema}`.`{$fk->reference_table}` (`{$fk->reference_column}`);", 'FOREIGN_KEY');
    }

    public function createView(string $view_name){
        $view = $this->schema->views[$view_name] ?? null;

        if(!$view){
            throw new \Exception("View {$view_name} does not exists in the Schema");
        }

        $this->add("CREATE OR REPLACE VIEW `{$this->schema->name}`.`{$view->name}` AS $view->def;", 'VIEW');
    }

    public function importData(string $table_name, string $identifier, bool $update = false){
        $data = $this->schema->data[$table_name] ?? null;
        
        if(!$data){
            throw new \Exception("Data for {$table_name} does not exists in the Schema");
        }
        
        $content = $data->getContent($identifier);
        
        if(empty($content)){
            return;
        }

        $format_value = function($value){
            if(is_string($value)){
                return "'{$value}'";
            }
            else if(is_null($value)){
                return "NULL";
            }
            else{
                return $value;
            }
        };

        if($update){
            $row = [];     
            $identifiers = $data->getIdentifierColumns();
            
            foreach($content as $column => $value){
                if(!in_array($column, $identifiers)){
                    $row[] = "{$column} = ".$format_value($value);
                }
            }

            $clause = [];
            foreach($identifiers as $identifier){
                $clause[] = "{$identifier} = ".$format_value($content[$identifier]);    
            }
            
            $columns_row = implode(', ', $row); 
            $columns_where = implode(' AND ', $clause); 
            $this->add("UPDATE `{$this->schema->name}`.`$data->table_name` SET {$columns_row} WHERE {$columns_where};", 'DATA');
        }
        else{
            $row = [];
            foreach($content as $column){
                $row[] = $format_value($column);
            }                            

            $columns_name = implode(',', array_keys($content)); 
            $columns_row = implode(',', $row); 

            $this->add("INSERT INTO `{$this->schema->name}`.`$data->table_name` ($columns_name) VALUES ({$columns_row});", 'DATA');
        }
    }

    public function dropTable(string $table_name){
        $this->add("DROP TABLE `{$this->schema->name}`.`{$table_name}`;", 'DROP_TABLE');    
    }
    
    public function dropView(string $view_name){
        $this->add("DROP VIEW `{$this->schema->name}`.`{$view_name}`;", 'DROP_VIEW');    
    }

    public function dropColumn(string $table_name, string $column_name){
        $this->add("ALTER TABLE `{$this->schema->name}`.`{$table_name}` DROP COLUMN `{$column_name}`;", 'DROP_COLUMN');    
    }

    public function dropKey(string $table_name, string $key_name){
        $this->add("ALTER TABLE `{$this->schema->name}`.`{$table_name}` DROP INDEX `{$key_name}`;", 'DROP_KEY');    
    }

    public function dropForeignKey(string $table_name, string $fk_name){
        $this->add("ALTER TABLE `{$this->schema->name}`.`{$table_name}` DROP FOREIGN KEY `{$fk_name}`;", 'DROP_FK');    
    }

    private static function formatConstantValue(string $value){
        $constant_values = [
            'CURRENT_TIMESTAMP'
        ];

        if(in_array(strtoupper($value), $constant_values)){
            return $value;
        }

        return "'{$value}'";            
    }

}
