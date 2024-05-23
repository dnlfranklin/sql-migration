<?php

namespace SqlMigration\Catalog;

class View{

    private $name;
    private $def;
    private $columns = [];

    public function __construct(string $name, string $def){
        $this->name = $name;
        $this->def = $def;
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function addColumn(string $column):self {
        $this->columns[] = $column;

        return $this;
    }
    
}   