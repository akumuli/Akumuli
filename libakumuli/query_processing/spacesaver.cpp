#include "spacesaver.h"

namespace Akumuli {
namespace QP {

static QueryParserToken<SpaceSaver<false>> fi_token("frequent-items");
static QueryParserToken<SpaceSaver<true>>  hh_token("heavy-hitters");

}}  // namespace
