# Coding style of EloqDB
Refer to Google Coding Style: https://google.github.io/styleguide/cppguide.html
1. Using Clang-format to format your code. Recommend to enable 'format on save' feature on VS code.
2. Using unique_ptr instead of raw pointer.
3. Using memory_order_relaxed should be careful. memory_order_relaxed allows code/instruction reorder by both compiler and CPU. In most case, global sequencial consisitency is not needed, hence std::memory_order_acquire and std::memory_order_release is enough.
4. Comments outside function use multiline style: /**  */(vs code will automatically generate comments if you press /**). Comments inside function uses style: //
5. Naming style: Function/Class name: MyName. Local variable: my_name. Class member variable: my_name_.
6. Include what you use. Don't reply on transitive inclusions.
7. Avoid using forward declarations where possible. Instead, include the headers you need.
```
// In a C++ source file:
class B;
void FuncInB();
extern int variable_in_b;
```
8. Define functions inline only when they are small, say, 10 lines or fewer. Or it will increase code size.
9. Include headers in the following order: Related header(foo.cc->foo.h), C system headers, C++ standard library headers, other libraries' headers, your project's headers. Separate by blank line.
10. Place code in a namespace, e.g. txservice, txlog etc.
11. Place a function's variables in the narrowest scope possible, and initialize variables in the declaration, and as close to the first use as possible.
12. Objects(like global and static variable) with static storage duration are forbidden unless they are trivially destructible. For example, std::map, std::string, unique_ptr etc. are not allowed to be global variables.
13. Use a struct only for passive objects that carry data; everything else is a class.  All fields must be public.
14. A class's public API must make clear whether the class is copyable, move-only, or neither copyable nor movable.
15. Prefer to use a struct instead of a pair or a tuple whenever the elements can have meaningful names.
16. Composition is often more appropriate than inheritance. When using inheritance, make it public. Explicitly annotate overrides of virtual functions or virtual destructors with exactly one of an override specifier.
17. Make classes' data members private, unless they are constants.
18. Group similar declarations together, placing public parts earlier.
19. Prefer using return values over output parameters: they improve readability, and often provide the same or better performance. Prefer to return by value or, failing that, return by reference. Avoid returning a pointer unless it can be null.
20. Prefer small and focused functions (below 40 lines).
21. Use C++-style casts like static_cast<float>(double_value), or brace initialization for conversion of arithmetic types like int64_t y = int64_t{1} << 42. Do not use cast formats like (int)x unless the cast is to void. Use const_cast to remove the const qualifier. Avoid use dynamic_cast.
22. class member use {} to initialize.
23. Use prefix increment/decrement(++i), unless the code explicitly needs the result of the postfix increment/decrement(i++) expression.
24. In APIs, use const whenever it makes sense. constexpr is a better choice for some uses of const.
25. We use int very often, for integers we know are not going to be too big, e.g., loop counters. Use plain old int for such things. You should assume that an int is at least 32 bits, but don't assume that it has more than 32 bits. If you need a 64-bit integer type, use int64_t or uint64_t.
26. Avoid defining macros, especially in headers; prefer inline functions, enums, and const variables. Name macros with a project-specific prefix.
27. Use nullptr for pointers, and '\0' for chars (and not the 0 literal).
28. The fundamental rule is: use type deduction only to make the code clearer or safer(avoids the possibility of unintended copies), and do not use it merely to avoid the inconvenience of writing an explicit type.
C++ code is usually clearer when types are explicit, especially when type deduction would depend on information from distant parts of the code. In expressions like:
```
auto foo = x.add_foo();
auto i = y.Find(key);
````
it may not be obvious what the resulting types are if the type of y isn't very well known, or if y was declared many lines earlier.
29. Enumerators should be named like constants, not like macros. That is, use kEnumName not ENUM_NAME.
```
enum class UrlTableError {
  kOk = 0,
  kOutOfMemory,
  kMalformedInput,
};
```
30. When sufficiently separated (e.g., .h and .cc files), comments describing the use of the class should go together with its interface definition(WHAT IT DOES); comments about the class operation and implementation should accompany the implementation of the class's methods.(HOW IT DOES)
