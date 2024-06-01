<?php

namespace SqlMigration\Core;

use SqlMigration\Catalog\Schema;

class Thresher{

    public static function clean(Schema $schema_up, Schema $schema_base = null):Differ {
        $differ = new Differ;

        if($schema_base){
            foreach($schema_base->tables as $table_name => $table){
                $table_up = $schema_up->tables[$table_name] ?? null;

                if(!$table_up){
                    $differ->add("DROP TABLE {$table_name}", 'DROP TABLE', $table_name, $schema_base->name, null, $table_name);

                    continue;
                }
                
                foreach($table->fks as $fk_name => $fk){
                    if(empty($table_up->fks[$fk_name])){
                        $differ->add("DROP FOREIGN KEY {$table_name}.{$fk_name}", 'DROP FOREIGN KEY', $fk_name, $table_name, null, $fk_name);
                    }
                }

                foreach($table->keys as $key_name => $key){
                    if(empty($table_up->keys[$key_name])){
                        $differ->add("DROP KEY {$table_name}.{$key_name}", 'DROP KEY', $key_name, $table_name, null, $key_name);  
                    }
                }

                foreach($table->columns as $column_name => $column){
                    if(empty($table_up->columns[$column_name])){
                        $differ->add("DROP COLUMN {$table_name}.{$column_name}", 'DROP COLUMN', $column_name, $table_name, null, $column_name);
                    }
                }
            }

            foreach($schema_base->views as $view_name => $view){
                $view_up = $schema_up->views[$view_name] ?? null;

                if(!$view_up){
                    $differ->add("DROP VIEW {$view_name}", 'DROP VIEW', $view_name, $schema_base->name, null, $view_name);
                }    
            }
        }

        return $differ;
    }

}