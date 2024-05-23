<?php

namespace SqlMigration\Catalog;

class ForeignKey{

    private $name;
    private $fk_column;
    private $reference_schema;
    private $reference_table;
    private $reference_column;

    public function __construct(
        string $name, 
        string $fk_column, 
        string $reference_schema, 
        string $reference_table, 
        string $reference_column
    ){
        $this->name = $name;
        $this->fk_column = $fk_column;
        $this->reference_schema = $reference_schema;
        $this->reference_table = $reference_table;
        $this->reference_column = $reference_column;
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }
    
}   