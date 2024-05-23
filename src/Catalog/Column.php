<?php

namespace SqlMigration\Catalog;

class Column{

    private $name;
    private $type;
    private $length = null;
    private $precision = 0;
    private $default = null;
    private $nullable = true;
    private $auto_increment = false;
    private $zerofill = false;
    private $unsigned = false;

    public function __construct(string $name, string $type, int $length = null, int $precision = null, $default = null){
        $this->name = $name;
        $this->type = strtoupper($type);
        $this->length = $length;
        $this->precision = $precision;
        $this->default = $default;
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function setNullable(bool $bool):void {
        $this->nullable = $bool;
    }

    public function setAutoIncrement(bool $bool):void {
        $this->auto_increment = $bool;
    }

    public function setZeroFill(bool $bool):void {
        $this->zerofill = $bool;
    }

    public function setUnsigned(bool $bool):void {
        $this->unsigned = $bool;
    }
        
}   