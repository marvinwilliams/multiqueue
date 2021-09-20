/**
******************************************************************************
* @file:   value.hpp
*
* @author: Marvin Williams
* @date:   2021/09/10 11:48
* @brief:  
*******************************************************************************
**/

#pragma once
#ifndef VALUE_HPP_INCLUDED
#define VALUE_HPP_INCLUDED

namespace multiqueue {

template <typename Key, typename T>
struct Value {
    using key_type = Key;
    using mapped_type = T;
    key_type key;
    mapped_type data;
};

}

#endif  //!VALUE_HPP_INCLUDED
