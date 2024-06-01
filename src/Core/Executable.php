<?php

namespace SqlMigration\Core;

class Executable{

    private $status  = 'NOT EXECUTED';
    private $details = '';

    public function __construct(private string $cmd, private string $group){}

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function hasSuccess(){
        return $this->status == 'EXECUTED';
    }

    public function success(){
        $this->status = 'EXECUTED';
    }

    public function error(string $details){
        $this->status  = 'ERROR';
        $this->details = $details;
    }

}