<?php

namespace SqlMigration\Catalog;

class Data{

    private $table_name;
    private $columns_name = [];
    private $columns_data = [];

    public function __construct(string $table_name){
        $this->table_name  = $table_name;    
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function setColumns(string ...$columns):self {
        $this->columns_name = $columns;

        return $this;
    }

    public function add(mixed ...$columns):self {
        if(count($columns) != count($this->columns_name)){
            throw new \Exception('Incompatible number of columns');
        }
        
        $this->columns_data[] = $columns;

        return $this;
    }
    
}   