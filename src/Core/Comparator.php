<?php

namespace SqlMigration\Core;

use SqlMigration\Catalog\Schema;

class Comparator{

    public static function compare(Schema $schema_up, Schema $schema_base = null):Differ {
        $differ = new Differ;
        
        if(!$schema_base){
            $differ->add('DATABASE '.$schema_up->name, 'SCHEMA', $schema_up->name, null, $schema_up->name, null);

            $schema_base = new Schema($schema_up->name);
        }
        else{
            $differ->add('DATABASE '.$schema_up->name, 'SCHEMA', $schema_up->name, null, $schema_up->name, $schema_base->name);
            
            if($schema_up->charset){
                $differ->add('DATABASE CHARSET', 'SCHEMA CHARSET', $schema_up->name, null, $schema_up->charset, $schema_base->charset);     
            }
            
            if($schema_up->collation){
                $differ->add('DATABASE COLLATION', 'SCHEMA COLLATION', $schema_up->name, null, $schema_up->collation, $schema_base->collation);
            }

            $drop_tables = $schema_up->drop->tables;
            if(!empty($drop_tables)){
                foreach($drop_tables as $drop_table){
                    $drop_table_name = $drop_table['name'];
                    if(!empty($schema_base->tables[$drop_table_name])){
                        $differ->add("DROP TABLE {$drop_table_name}", 'DROP TABLE', $drop_table_name, $schema_up->name, null, $drop_table_name);    
                    }
                }
            }
            
            $drop_views = $schema_up->drop->views;
            if(!empty($drop_views)){
                foreach($drop_views as $drop_view){
                    $drop_view_name = $drop_view['name'];
                    if(!empty($schema_base->views[$drop_view_name])){
                        $differ->add("DROP VIEW {$drop_view_name}", 'DROP VIEW', $drop_view_name, $schema_up->name, null, $drop_view_name);    
                    }
                }
            }

            $drop_columns = $schema_up->drop->columns;
            if(!empty($drop_columns)){
                foreach($drop_columns as $drop_column){
                    $drop_table_name = $drop_column['table'];
                    $drop_column_name = $drop_column['name'];

                    if(!empty($schema_base->tables[$drop_table_name]->columns[$drop_column_name])){
                        $differ->add("DROP COLUMN {$drop_table_name}.{$drop_column_name}", 'DROP COLUMN', $drop_column_name, $drop_table_name, null, $drop_column_name);    
                    }
                }
            }

            $drop_keys = $schema_up->drop->keys;
            if(!empty($drop_keys)){
                foreach($drop_keys as $drop_key){
                    $drop_table_name = $drop_key['table'];
                    $drop_key_name = $drop_key['name'];

                    if(!empty($schema_base->tables[$drop_table_name]->keys[$drop_key_name])){
                        $differ->add("DROP KEY {$drop_table_name}.{$drop_key_name}", 'DROP KEY', $drop_key_name, $drop_table_name, null, $drop_key_name);    
                    }
                }
            }

            $drop_fks = $schema_up->drop->fks;
            if(!empty($drop_fks)){
                foreach($drop_fks as $drop_fk){
                    $drop_table_name = $drop_fk['table'];
                    $drop_fk_name = $drop_fk['name'];

                    if(!empty($schema_base->tables[$drop_table_name]->fks[$drop_fk_name])){
                        $differ->add("DROP FOREIGN KEY {$drop_table_name}.{$drop_fk_name}", 'DROP FOREIGN KEY', $drop_fk_name, $drop_table_name, null, $drop_fk_name);    
                    }
                }
            }
        }        
        
        foreach ($schema_up->tables as $table) {
            if(empty($schema_base->tables[$table->name])){
                $differ->add('TABLE '.$table->name, 'TABLE', $table->name,  $schema_up->name, "{$table->name} {$table->engine} {$table->charset} {$table->collation} {$table->comments}", null);
            }
            else{
                $table_base = empty($schema_base->tables[$table->name]) ? null : $schema_base->tables[$table->name];
                
                $differ->add('TABLE '.$table->name, 'TABLE', $table->name, $schema_up->name, "{$table->name} {$table->engine} {$table->charset} {$table->collation} {$table->comments}", "{$table_base->name} {$table_base->engine} {$table_base->charset} {$table_base->collation} {$table_base->comments}");

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
                        $differ->add("COLUMN {$table->name}.{$column->name}", 'COLUMN', $column->name, $table->name, $column_value, null);  
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

                        $differ->add("COLUMN {$table->name}.{$column->name}", 'COLUMN', $column->name, $table->name, $column_value, $column_base_value);
                    }
                }
            }             
            
            if(!empty($schema_base->tables[$table->name])){
                foreach($table->keys as $key){
                    if(empty($schema_base->tables[$table->name]->keys[$key->name])){
                        $differ->add("KEY $key->name ON {$table->name}", 'KEY', $key->name, $table->name, "{$key->type} {$key->columns}", null);
                    }
                    else{
                        $key_base = $schema_base->tables[$table->name]->keys[$key->name];
                        
                        $differ->add("KEY {$key->name} ON {$table->name}", 'KEY', $key->name, $table->name, "{$key->type} {$key->columns}", "{$key_base->type} {$key_base->columns}");
                    }                
                }
            }

            foreach($table->fks as $fk){
                if(empty($schema_base->tables[$table->name]) || empty($schema_base->tables[$table->name]->fks[$fk->name])){
                    $differ->add("FOREIGN KEY $fk->name ON {$table->name}", 'FOREIGN KEY', $fk->name, $table->name, "{$table->name}.{$fk->fk_column} references {$fk->reference_schema}.{$fk->reference_table}.{$fk->reference_column}", null);
                }
                else{
                    $fk_base = $schema_base->tables[$table->name]->fks[$fk->name];
                    
                    $differ->add("FOREIGN KEY $fk->name ON {$table->name}", 'FOREIGN KEY', $fk->name, $table->name, "{$table->name}.{$fk->fk_column} references {$fk->reference_schema}.{$fk->reference_table}.{$fk->reference_column}", "{$table_base->name}.{$fk_base->fk_column} references {$fk_base->reference_schema}.{$fk_base->reference_table}.{$fk_base->reference_column}");
                }                
            }
            
        }  
        
        foreach ($schema_up->views as $view) {
            if(empty($schema_base->views[$view->name])){
                $differ->add("VIEW $view->name", 'VIEW', $view->name, $schema_up->name, implode(',', $view->columns), null);
            }
            else{
                $view_base = $schema_base->views[$view->name];

                $differ->add("VIEW $view->name", 'VIEW', $view->name, $schema_up->name, implode(',', $view->columns), implode(',', $view_base->columns));
            }            
        }

        foreach($schema_up->data as $data_table_name => $data){
            $array_hashes_up = call_user_func_array([$data, 'getHashes'], $data->columns_name);
            
            if(empty($schema_base->data[$data_table_name])){
                foreach($array_hashes_up as $key => $value){
                    $differ->add("DATA {$data_table_name}", 'DATA', $key, $data_table_name, $value, null);    
                }                    
            }
            else{
                $data_base = $schema_base->data[$data_table_name];
                call_user_func_array([$data_base, 'setIdentifierColumns'], $data->identifier_columns);
                
                $array_hashes_base = call_user_func_array([$data_base, 'getHashes'], $data->columns_name);
                
                foreach($array_hashes_up as $key => $value){
                    if(empty($array_hashes_base[$key])){
                        $differ->add("DATA {$data_table_name}", 'DATA', $key, $data_table_name, $value, null);    
                    }
                    else{
                        $differ->add("DATA {$data_table_name}", 'DATA', $key, $data_table_name, $value, $array_hashes_base[$key]);
                    }
                }
            }
        }

        return $differ;
    }

}