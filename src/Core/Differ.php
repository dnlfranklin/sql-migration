<?php

namespace SqlMigration\Core;

use SqlMigration\Catalog\Schema;

class Differ{

    private $schema_up;
    private $schema_base;
    private $diff = [];

    public function __construct(Schema $schema_up, Schema $schema_base = null){
        $this->schema_up = $schema_up;
        
        if($schema_base){
            $this->schema_base = $schema_base;
        }
    }

    public function analyze():Array {
        $diff = [];

        if(!$this->schema_base){
            $this->add('DATABASE '.$this->schema_up->name, 'SCHEMA', $this->schema_up->name, null, $this->schema_up->name, null);

            $this->schema_base = new Schema($this->schema_up->name);
        }
        else{
            $this->add('DATABASE '.$this->schema_up->name, 'SCHEMA', $this->schema_up->name, null, $this->schema_up->name, $this->schema_base->name);
            
            if($this->schema_up->charset){
                $this->add('DATABASE CHARSET', 'SCHEMA CHARSET', $this->schema_up->name, null, $this->schema_up->charset, $this->schema_base->charset);     
            }
            
            if($this->schema_up->collation){
                $this->add('DATABASE COLLATION', 'SCHEMA COLLATION', $this->schema_up->name, null, $this->schema_up->collation, $this->schema_base->collation);
            }

            $drop_tables = $this->schema_up->drop->tables;
            if(!empty($drop_tables)){
                foreach($drop_tables as $drop_table){
                    $drop_table_name = $drop_table['name'];
                    if(!empty($this->schema_base->tables[$drop_table_name])){
                        $this->add("DROP TABLE {$drop_table_name}", 'DROP TABLE', $drop_table_name, $this->schema_up->name, null, $drop_table_name);    
                    }
                }
            }

            $drop_columns = $this->schema_up->drop->columns;
            if(!empty($drop_columns)){
                foreach($drop_columns as $drop_column){
                    $drop_table_name = $drop_column['table'];
                    $drop_column_name = $drop_column['name'];

                    if(!empty($this->schema_base->tables[$drop_table_name]->columns[$drop_column_name])){
                        $this->add("DROP COLUMN {$drop_table_name}.{$drop_column_name}", 'DROP COLUMN', $drop_column_name, $drop_table_name, null, $drop_column_name);    
                    }
                }
            }

            $drop_keys = $this->schema_up->drop->keys;
            if(!empty($drop_keys)){
                foreach($drop_keys as $drop_key){
                    $drop_table_name = $drop_key['table'];
                    $drop_key_name = $drop_key['name'];

                    if(!empty($this->schema_base->tables[$drop_table_name]->keys[$drop_key_name])){
                        $this->add("DROP KEY {$drop_table_name}.{$drop_key_name}", 'DROP KEY', $drop_key_name, $drop_table_name, null, $drop_key_name);    
                    }
                }
            }

            $drop_fks = $this->schema_up->drop->fks;
            if(!empty($drop_fks)){
                foreach($drop_fks as $drop_fk){
                    $drop_table_name = $drop_fk['table'];
                    $drop_fk_name = $drop_fk['name'];

                    if(!empty($this->schema_base->tables[$drop_table_name]->fks[$drop_fk_name])){
                        $this->add("DROP FOREIGN KEY {$drop_table_name}.{$drop_fk_name}", 'DROP FOREIGN KEY', $drop_fk_name, $drop_table_name, null, $drop_fk_name);    
                    }
                }
            }
        }        
        
        foreach ($this->schema_up->tables as $table) {
            if(empty($this->schema_base->tables[$table->name])){
                $this->add('TABLE '.$table->name, 'TABLE', $table->name,  $this->schema_up->name, "{$table->name} {$table->engine} {$table->charset} {$table->collation} {$table->comments}", null);
            }
            else{
                $table_base = empty($this->schema_base->tables[$table->name]) ? null : $this->schema_base->tables[$table->name];
                
                $this->add('TABLE '.$table->name, 'TABLE', $table->name, $this->schema_up->name, "{$table->name} {$table->engine} {$table->charset} {$table->collation} {$table->comments}", "{$table_base->name} {$table_base->engine} {$table_base->charset} {$table_base->collation} {$table_base->comments}");

                foreach($table->columns as $column){
                    if(is_null($column->length) && is_null($column->precision)){
                        $length_precision = '';    
                    }
                    else if(!is_null($column->length) && !is_null(!$column->precision) && $column->precision != 0){
                        $length_precision = "({$column->length},{$column->precision})";
                    }
                    else{
                        $length_precision = is_null($column->length) ? "({$column->precision})" : "({$column->length})";
                    }

                    $column_value = "{$column->name} {$column->type}{$length_precision}";
                    $column_value.= $column->nullable ? '' : ' NOT NULL';
                    $column_value.= is_null($column->default) ? '' : ' DEFAULT '.$column->default;
                    $column_value.= $column->unsigned ? ' unsigned' : '';
                    $column_value.= $column->zerofill ? ' zerofill' : '';
                    $column_value.= $column->auto_increment ? ' AUTO_INCREMENT' : '';

                    if(empty($table_base->columns[$column->name])){
                        $this->add("COLUMN {$table->name}.{$column->name}", 'COLUMN', $column->name, $table->name, $column_value, null);  
                    }
                    else{
                        $column_base = $table_base->columns[$column->name];

                        if(is_null($column_base->length) && is_null($column_base->precision)){
                            $length_precision = '';    
                        }
                        else if(!is_null($column_base->length) && !is_null(!$column_base->precision) && $column_base->precision != 0){
                            $length_precision = "({$column_base->length},{$column_base->precision})";
                        }
                        else{
                            $length_precision = is_null($column_base->length) ? "({$column_base->precision})" : "({$column_base->length})";
                        }

                        $column_base_value = "{$column_base->name} {$column_base->type}{$length_precision}";
                        $column_base_value.= $column_base->nullable ? '' : ' NOT NULL';
                        $column_base_value.= is_null($column_base->default) ? '' : ' DEFAULT '.$column_base->default;
                        $column_base_value.= $column_base->unsigned ? ' unsigned' : '';
                        $column_base_value.= $column_base->zerofill ? ' zerofill' : '';
                        $column_base_value.= $column_base->auto_increment ? ' AUTO_INCREMENT' : '';

                        $this->add("COLUMN {$table->name}.{$column->name}", 'COLUMN', $column->name, $table->name, $column_value, $column_base_value);
                    }
                }
            }             
            
            if(!empty($this->schema_base->tables[$table->name])){
                foreach($table->keys as $key){
                    if(empty($this->schema_base->tables[$table->name]->keys[$key->name])){
                        $this->add("KEY $key->name ON {$table->name}", 'KEY', $key->name, $table->name, "{$key->type} {$key->columns}", null);
                    }
                    else{
                        $key_base = $this->schema_base->tables[$table->name]->keys[$key->name];
                        
                        $this->add("KEY {$key->name} ON {$table->name}", 'KEY', $key->name, $table->name, "{$key->type} {$key->columns}", "{$key_base->type} {$key_base->columns}");
                    }                
                }
            }

            foreach($table->fks as $fk){
                if(empty($this->schema_base->tables[$table->name]) || empty($this->schema_base->tables[$table->name]->fks[$fk->name])){
                    $this->add("FOREIGN KEY $fk->name ON {$table->name}", 'FOREIGN KEY', $fk->name, $table->name, "{$table->name}.{$fk->fk_column} references {$fk->reference_schema}.{$fk->reference_table}.{$fk->reference_column}", null);
                }
                else{
                    $fk_base = $this->schema_base->tables[$table->name]->fks[$fk->name];
                    
                    $this->add("FOREIGN KEY $fk->name ON {$table->name}", 'FOREIGN KEY', $fk->name, $table->name, "{$table->name}.{$fk->fk_column} references {$fk->reference_schema}.{$fk->reference_table}.{$fk->reference_column}", "{$table_base->name}.{$fk_base->fk_column} references {$fk_base->reference_schema}.{$fk_base->reference_table}.{$fk_base->reference_column}");
                }                
            }
            
        }  
        
        foreach ($this->schema_up->views as $view) {
            if(empty($this->schema_base->views[$view->name])){
                $this->add("VIEW $view->name", 'VIEW', $view->name, $this->schema_up->name, implode(',', $view->columns), null);
            }
            else{
                $view_base = $this->schema_base->views[$view->name];

                $this->add("VIEW $view->name", 'VIEW', $view->name, $this->schema_up->name, implode(',', $view->columns), implode(',', $view_base->columns));
            }            
        }

        return $this->diff;
    }

    private function add(
        string $title, 
        string $catalog, 
        string $identifier,
        ?string $parent, 
        ?string $expected_value, 
        ?string $current_value
    ){
        $this->diff[] = [
            'title' => $title,
            'catalog' => $catalog,
            'identifier' => $identifier,
            'parent' => $parent,
            'expected' => $expected_value,    
            'current' => $current_value,
            "action" => $expected_value == $current_value ? null : ($current_value ? 'UPDATE' : 'CREATE'),
        ];                
    }

}