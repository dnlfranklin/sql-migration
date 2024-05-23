<?php

namespace SqlMigration\Catalog;

class Drop{

    private $tables = [];
    private $columns = [];
    private $keys = [];
    private $fks = [];

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function table(string $table):void {
        $this->tables[] = [
            'name' => $table
        ];
    }

    public function column(string $table, string $column):void {
        $this->columns[] = [
            'table' => $table,
            'name' => $column
        ];
    }

    public function key(string $table, string $key):void {
        $this->keys[] = [
            'table' => $table,
            'name' => $key
        ];
    }

    public function fk(string $table, string $key):void {
        $this->fks[] = [
            'table' => $table,
            'name' => $key
        ];
    }
    
}   