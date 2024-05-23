<?php

namespace SqlMigration\Catalog;

class Key{

    private $name;
    private $columns;
    private $type;

    public function __construct(string $name, string $columns, string $type){
        $this->name = $name;
        $this->columns = $columns;
        $this->type = $type;
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }
    
}   